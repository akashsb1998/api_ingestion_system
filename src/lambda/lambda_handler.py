"""
lambda_handler.py
-----------------
The Lambda function that does the actual work:
  1. Read job details from SQS message
  2. Load checkpoint (where did last run stop?)
  3. Loop through API pages with rate limiting
  4. Write each page to S3
  5. Save checkpoint (where should next run start?)

This is the complete picture of what happens INSIDE each Lambda.
Airflow never sees any of this — Lambda owns the entire fetch+write flow.
"""

import json
import time
import uuid
import logging
import boto3
import redis
import requests

from datetime import datetime, timezone
from src.redis.token_bucket import RateLimiterRegistry
from src.checkpoint.checkpoint_manager import CheckpointManager, Checkpoint

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ── AWS clients (created once at cold start, reused across invocations) ──────
s3_client = boto3.client("s3")

# ── Redis connection (ElastiCache in prod, localhost for local testing) ───────
redis_client = redis.Redis(
    host="your-elasticache-endpoint.cache.amazonaws.com",
    port=6379,
    decode_responses=True,
    socket_connect_timeout=2,
)

rate_limiter = RateLimiterRegistry(redis_client)
checkpoint_mgr = CheckpointManager(table_name="api_checkpoints")


# ── Main Lambda entry point ───────────────────────────────────────────────────

def lambda_handler(event, context):
    """
    AWS calls this function when an SQS message is available.

    The 'event' contains the SQS message(s). We process one job per invocation.
    """
    for record in event["Records"]:
        job = json.loads(record["body"])
        logger.info(f"Processing job: {job['api_name']}")

        try:
            process_api_job(job)
        except Exception as e:
            logger.error(f"{job['api_name']}: job failed — {e}")
            checkpoint_mgr.mark_failed(job["api_name"], str(e))
            raise  # re-raise so SQS knows to retry (message becomes visible again)

    return {"statusCode": 200}


# ── Core job processor ────────────────────────────────────────────────────────

def process_api_job(job: dict):
    """
    Full lifecycle for one API fetch job.

    job = {
        "api_name": "api1_fx_rates",
        "base_url": "https://api.fxrates.com/v1/rates",
        "rpm": 10,
        "s3_bucket": "raw-landing",
        "s3_prefix": "source=fx_rates"
    }
    """
    api_name = job["api_name"]
    run_id = str(uuid.uuid4())  # unique ID for this run — used in S3 key

    # ── Step 1: Load checkpoint ───────────────────────────────────────────────
    # Find out where the last run stopped.
    # If first ever run → cursor is None → fetch from the beginning.
    checkpoint = checkpoint_mgr.load(api_name)
    cursor = checkpoint.last_cursor

    logger.info(f"{api_name}: starting from cursor={cursor}")

    # ── Step 2: Fetch all pages ───────────────────────────────────────────────
    all_records = []
    page_num = 0
    last_cursor = None

    while True:
        page_num += 1

        # RATE LIMIT CHECK — before every single HTTP call
        # If bucket is empty, this blocks here (sleeps) until tokens refill
        # The HTTP call below only runs after a token is acquired
        rate_limiter.acquire(api_name, rpm=job["rpm"])

        # Make the HTTP call
        response = _call_api(
            base_url=job["base_url"],
            cursor=cursor,
            page_size=job.get("page_size", 100),
            api_name=api_name,
        )

        if response is None:
            break  # unrecoverable error — checkpoint stays unchanged, SQS retries

        records = response.get("records", [])
        all_records.extend(records)

        logger.info(
            f"{api_name}: page {page_num} fetched — "
            f"{len(records)} records (total so far: {len(all_records)})"
        )

        # ── Step 3: Write this page to S3 immediately ─────────────────────────
        # We write page by page rather than buffering everything in memory.
        # Reason: Lambda has limited memory (128MB–10GB). Large APIs would crash it.
        _write_page_to_s3(
            records=records,
            bucket=job["s3_bucket"],
            prefix=job["s3_prefix"],
            api_name=api_name,
            run_id=run_id,
            page_num=page_num,
        )

        # ── Step 4: Check if there are more pages ─────────────────────────────
        next_token = response.get("next_page_token")

        if not next_token:
            # No more pages — we're done
            last_cursor = None  # signals "fully caught up"
            logger.info(f"{api_name}: all pages fetched ({page_num} pages total)")
            break

        cursor = next_token        # use this token to get the next page
        last_cursor = next_token   # save in case Lambda crashes on next iteration

    # ── Step 5: Save checkpoint ───────────────────────────────────────────────
    # Only reached if ALL pages fetched and written successfully.
    # If Lambda crashes before this, checkpoint stays at previous value → safe retry.
    final_checkpoint = Checkpoint(
        api_name=api_name,
        last_cursor=_get_next_run_cursor(all_records, job),
        last_run_at=datetime.now(timezone.utc).isoformat(),
        records_fetched=len(all_records),
        status="success",
    )
    checkpoint_mgr.save(final_checkpoint)

    logger.info(
        f"{api_name}: ✓ complete — {len(all_records)} records "
        f"across {page_num} pages, written to S3"
    )


# ── HTTP call with retry on 429 ───────────────────────────────────────────────

def _call_api(base_url: str, cursor: str, page_size: int, api_name: str) -> dict | None:
    """
    Make one HTTP GET request to the API.

    Handles:
    - 200: success, return response JSON
    - 429: API rate limited us anyway — read Retry-After header and wait
    - 5xx: server error — retry with backoff
    - Other errors: log and return None
    """
    params = {"limit": page_size}
    if cursor:
        params["page_token"] = cursor  # send the bookmark back to the API

    headers = _get_auth_headers(api_name)

    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.get(
                base_url,
                params=params,
                headers=headers,
                timeout=30,
            )

            if response.status_code == 200:
                return response.json()

            elif response.status_code == 429:
                # API said "too many requests" despite our token bucket.
                # This can happen if the API has a stricter per-second sub-limit.
                retry_after = int(response.headers.get("Retry-After", 60))
                logger.warning(
                    f"{api_name}: 429 received, waiting {retry_after}s "
                    f"(Retry-After header)"
                )
                time.sleep(retry_after)
                continue  # retry the same page

            elif response.status_code >= 500:
                # Server error — wait with exponential backoff + jitter
                # Jitter: each Lambda waits a slightly different amount
                # so they don't all retry at exactly the same second
                wait = (2 ** attempt) + (0.1 * attempt)
                logger.warning(
                    f"{api_name}: {response.status_code} server error, "
                    f"retrying in {wait:.1f}s (attempt {attempt + 1})"
                )
                time.sleep(wait)
                continue

            else:
                logger.error(
                    f"{api_name}: unexpected status {response.status_code} — "
                    f"{response.text[:200]}"
                )
                return None

        except requests.exceptions.Timeout:
            logger.warning(f"{api_name}: request timed out (attempt {attempt + 1})")
            time.sleep(2 ** attempt)

        except requests.exceptions.ConnectionError as e:
            logger.error(f"{api_name}: connection error — {e}")
            return None

    logger.error(f"{api_name}: all {max_retries} retries exhausted")
    return None


# ── S3 write ──────────────────────────────────────────────────────────────────

def _write_page_to_s3(
    records: list,
    bucket: str,
    prefix: str,
    api_name: str,
    run_id: str,
    page_num: int,
):
    """
    Write one page of records to S3 as a JSON file.

    S3 key structure:
        source=fx_rates/
          date=2024-01-15/
            run_id=abc-123/
              page=001.json

    Why include run_id?
        If Lambda retries, it writes a NEW file (new run_id) instead of
        overwriting. This makes retries safe — no partial overwrites.
        The downstream Spark job deduplicates on record ID anyway.

    Why write page by page?
        Avoids holding all 10,000 records in Lambda memory.
        Each page is written immediately after fetching.
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    s3_key = (
        f"{prefix}/"
        f"date={today}/"
        f"run_id={run_id}/"
        f"page={page_num:04d}.json"
    )

    body = json.dumps({
        "api_name": api_name,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "page": page_num,
        "record_count": len(records),
        "records": records,
    })

    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )
    logger.info(f"Written to s3://{bucket}/{s3_key} ({len(records)} records)")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _get_next_run_cursor(records: list, job: dict) -> str | None:
    """
    Determine the cursor for the NEXT run.

    For time-based APIs: use the timestamp of the last record fetched.
    Next run will call: GET /rates?since=<this timestamp>

    For cursor-based APIs with full refresh: return None (start over next time).
    """
    if not records:
        return None

    # Most REST APIs support a "since" timestamp for incremental fetches
    last_record = records[-1]
    if "timestamp" in last_record:
        return last_record["timestamp"]
    if "updated_at" in last_record:
        return last_record["updated_at"]

    return None  # fallback: next run will fetch everything again


def _get_auth_headers(api_name: str) -> dict:
    """
    In production: fetch credentials from AWS Secrets Manager.
    Here: returns a placeholder for illustration.
    """
    # Production version:
    # secret = boto3.client("secretsmanager").get_secret_value(SecretId=f"api/{api_name}")
    # creds = json.loads(secret["SecretString"])
    # return {"Authorization": f"Bearer {creds['token']}"}

    return {"Authorization": "Bearer YOUR_TOKEN_HERE"}
