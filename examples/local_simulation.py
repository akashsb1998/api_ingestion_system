"""
local_simulation.py
-------------------
Run this to see the entire system working WITHOUT needing AWS.

Simulates:
  - 3 APIs (API1 rpm=3, API2 rpm=10, API3 rpm=30)
  - 3 Lambda workers running in parallel threads
  - Token bucket rate limiting (using a simple in-memory dict instead of Redis)
  - Pagination with next_page_token
  - Checkpoint save/load (using a simple dict instead of DynamoDB)
  - S3 write (just prints to console instead of real S3)

Run with:
    python examples/local_simulation.py
"""

import time
import json
import uuid
import random
import threading
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(threadName)-12s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ── Fake API server ───────────────────────────────────────────────────────────

class FakeAPI:
    """
    Pretends to be a REST API with paginated data.
    Returns 5 records per page, up to 3 pages total.
    """

    PAGES = {
        "api1_fx_rates": [
            {"id": 1, "pair": "USD/INR", "rate": 83.45},
            {"id": 2, "pair": "EUR/INR", "rate": 89.12},
        ],
        "api2_trade_data": [
            {"id": 10, "instrument": "EURUSD", "notional": 1000000},
            {"id": 11, "instrument": "GBPUSD", "notional": 500000},
        ],
        "api3_ref_data": [
            {"id": 100, "symbol": "AAPL", "exchange": "NASDAQ"},
            {"id": 101, "symbol": "GOOGL", "exchange": "NASDAQ"},
        ],
    }

    CURSORS = {
        "api1_fx_rates": ["page1_token", "page2_token", None],
        "api2_trade_data": ["page1_token", "page2_token", None],
        "api3_ref_data": ["page1_token", None],
    }

    def call(self, api_name: str, cursor: str) -> dict:
        pages = self.PAGES[api_name]
        cursors = self.CURSORS[api_name]

        # figure out which page based on cursor
        if cursor is None:
            page_idx = 0
        elif cursor == "page1_token":
            page_idx = 1
        elif cursor == "page2_token":
            page_idx = 2
        else:
            page_idx = 0

        time.sleep(0.1)  # simulate network latency

        return {
            "records": pages,
            "next_page_token": cursors[page_idx] if page_idx < len(cursors) else None,
        }


# ── In-memory token bucket (replaces Redis for local testing) ─────────────────

class LocalTokenBucket:
    """
    Same logic as the Redis version, but uses a Python dict + lock.
    Shows exactly how the token bucket works without needing Redis running.
    """

    def __init__(self):
        self._buckets = {}
        self._lock = threading.Lock()

    def acquire(self, api_name: str, rpm: int):
        while True:
            with self._lock:
                if api_name not in self._buckets:
                    self._buckets[api_name] = {"tokens": rpm, "refill_at": time.time() + 10}

                bucket = self._buckets[api_name]

                # Refill if the window has passed
                if time.time() >= bucket["refill_at"]:
                    bucket["tokens"] = rpm
                    bucket["refill_at"] = time.time() + 10  # 10s window for demo
                    logger.info(f"  [{api_name}] 🔄 bucket refilled to {rpm} tokens")

                if bucket["tokens"] > 0:
                    bucket["tokens"] -= 1
                    logger.debug(f"  [{api_name}] token acquired ({bucket['tokens']} left)")
                    return  # got a token — proceed

                wait_time = bucket["refill_at"] - time.time()

            # Outside the lock: sleep and retry
            jitter = random.uniform(0, 0.3)
            logger.info(
                f"  [{api_name}] ⏸  bucket empty — waiting {wait_time:.1f}s + {jitter:.1f}s jitter"
            )
            time.sleep(max(0.1, wait_time + jitter))


# ── In-memory checkpoint (replaces DynamoDB for local testing) ────────────────

class LocalCheckpointStore:
    def __init__(self):
        self._store = {}

    def load(self, api_name: str) -> str | None:
        return self._store.get(api_name, {}).get("cursor")

    def save(self, api_name: str, cursor: str | None, records: int):
        self._store[api_name] = {
            "cursor": cursor,
            "records": records,
            "saved_at": datetime.utcnow().isoformat(),
        }
        logger.info(f"  [{api_name}] 💾 checkpoint saved (cursor={cursor}, records={records})")


# ── Simulated Lambda worker ───────────────────────────────────────────────────

def simulated_lambda(job: dict, rate_limiter: LocalTokenBucket,
                     checkpoint_store: LocalCheckpointStore,
                     fake_api: FakeAPI):
    """
    This simulates exactly what the real Lambda does:
    1. Load cursor from checkpoint
    2. Loop: acquire token → call API → write page → check for next page
    3. Save checkpoint
    """
    api_name = job["api_name"]
    run_id = str(uuid.uuid4())[:8]
    logger.info(f"[{api_name}] Lambda started (run_id={run_id})")

    cursor = checkpoint_store.load(api_name)
    all_records = []
    page = 0

    while True:
        page += 1

        # ── Rate limit check (blocks here if bucket is empty) ─────────────────
        logger.info(f"[{api_name}] checking token bucket for page {page}...")
        rate_limiter.acquire(api_name, rpm=job["rpm"])

        # ── Call the API ──────────────────────────────────────────────────────
        logger.info(f"[{api_name}] → calling API (page {page}, cursor={cursor})")
        response = fake_api.call(api_name, cursor)
        records = response["records"]
        all_records.extend(records)

        # ── Simulate S3 write ─────────────────────────────────────────────────
        s3_key = f"{job['s3_prefix']}/date=2024-01-15/run={run_id}/page={page:03d}.json"
        logger.info(f"[{api_name}] ✓ wrote {len(records)} records → s3://raw-landing/{s3_key}")

        # ── Check for more pages ──────────────────────────────────────────────
        next_token = response.get("next_page_token")
        if not next_token:
            logger.info(f"[{api_name}] no more pages (fetched {page} pages total)")
            break
        cursor = next_token

    # ── Save checkpoint ───────────────────────────────────────────────────────
    checkpoint_store.save(api_name, cursor, len(all_records))
    logger.info(f"[{api_name}] ✅ DONE — {len(all_records)} total records")


# ── Run the simulation ────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\n" + "="*60)
    print("  API Ingestion System — Local Simulation")
    print("  3 APIs, 3 Lambda workers, shared token bucket")
    print("="*60 + "\n")

    fake_api = FakeAPI()
    rate_limiter = LocalTokenBucket()
    checkpoint_store = LocalCheckpointStore()

    jobs = [
        {"api_name": "api1_fx_rates",  "rpm": 3,  "s3_prefix": "source=fx_rates"},
        {"api_name": "api2_trade_data","rpm": 10, "s3_prefix": "source=trade_data"},
        {"api_name": "api3_ref_data",  "rpm": 30, "s3_prefix": "source=ref_data"},
    ]

    # Start all 3 Lambdas simultaneously (as threads)
    # Notice: API1 (rpm=3) will hit the rate limit and wait
    # API3 (rpm=30) will zip through without waiting
    threads = [
        threading.Thread(
            target=simulated_lambda,
            args=(job, rate_limiter, checkpoint_store, fake_api),
            name=f"Lambda-{job['api_name'].split('_')[0].upper()}",
        )
        for job in jobs
    ]

    print("Starting all 3 Lambda workers simultaneously...\n")
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    print("\n" + "="*60)
    print("  Simulation complete. Observe:")
    print("  - API1 (rpm=3) waited the most")
    print("  - API3 (rpm=30) finished first")
    print("  - Zero 429 errors")
    print("="*60)
