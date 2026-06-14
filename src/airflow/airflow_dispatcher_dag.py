"""
airflow_dispatcher_dag.py
-------------------------
Airflow's ONLY job in this system: read the API config list
and push one SQS message per API.

That's it. Airflow does NOT:
  - Call any API directly
  - Wait for HTTP responses
  - Write to S3
  - Hold open connections

It posts 50 messages in ~1 second and marks the task complete.
The Lambda functions handle everything else independently.

WHY THIS PATTERN (vs Airflow calling APIs directly)?
  - Airflow worker slots are limited and expensive
  - HTTP calls can take seconds to minutes (network, slow APIs)
  - If Airflow calls the API, a slow API blocks the worker slot
  - With SQS: Airflow finishes instantly, Lambda scales to 50 workers
    independently, slow APIs only block their own Lambda
"""

import json
import yaml
import boto3
import logging

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

# ── DAG default arguments ─────────────────────────────────────────────────────
default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": True,
    "email": ["data-eng@yourcompany.com"],
}


# ── DAG definition ────────────────────────────────────────────────────────────
@dag(
    dag_id="api_ingestion_dispatcher",
    description="Dispatches API fetch jobs to SQS for Lambda workers",
    schedule_interval="@hourly",
    start_date=days_ago(1),
    default_args=default_args,
    catchup=False,
    tags=["ingestion", "api", "sqs"],
)
def api_ingestion_dispatcher():

    @task
    def load_api_configs() -> list[dict]:
        """
        Read the list of 50 APIs from config file (or DynamoDB in prod).
        Returns a list of dicts — one per API.
        """
        with open("/opt/airflow/config/api_configs.yaml") as f:
            config = yaml.safe_load(f)

        apis = config["apis"]
        logger.info(f"Loaded {len(apis)} API configs")
        return apis

    @task
    def load_checkpoints(api_configs: list[dict]) -> list[dict]:
        """
        For each API, read its last checkpoint from DynamoDB.
        Attach the cursor to the config so Lambda knows where to resume.

        This is the "read the save point before starting the game" step.
        """
        dynamodb = boto3.resource("dynamodb", region_name="ap-south-1")
        table = dynamodb.Table("api_checkpoints")

        enriched_configs = []
        for api in api_configs:
            response = table.get_item(Key={"api_name": api["name"]})
            item = response.get("Item", {})

            api_with_cursor = {
                **api,
                "cursor": item.get("last_cursor"),        # None = start from beginning
                "last_run_at": item.get("last_run_at"),
                "s3_bucket": "raw-landing",
            }
            enriched_configs.append(api_with_cursor)

        return enriched_configs

    @task
    def dispatch_to_sqs(api_configs: list[dict]) -> dict:
        """
        Push one SQS message per API.

        Each message is a small JSON note with instructions for Lambda:
        - which API to call
        - the cursor (where to resume from)
        - where to write in S3

        After this task: Airflow is DONE. Lambda takes over.
        """
        sqs = boto3.client("sqs", region_name="ap-south-1")
        queue_url = "https://sqs.ap-south-1.amazonaws.com/123456789/api-fetch-jobs"

        sent = 0
        failed = 0

        for api in api_configs:
            message = {
                "api_name": api["name"],
                "base_url": api["base_url"],
                "rpm": api["rate_limit_rpm"],
                "page_size": api.get("page_size", 100),
                "cursor": api.get("cursor"),           # from checkpoint
                "s3_bucket": api["s3_bucket"],
                "s3_prefix": api["s3_prefix"],
                "dispatched_at": datetime.utcnow().isoformat(),
            }

            try:
                sqs.send_message(
                    QueueUrl=queue_url,
                    MessageBody=json.dumps(message),
                    # MessageGroupId groups messages by API for FIFO ordering
                    # This ensures API1's messages are processed in order
                    MessageGroupId=api["name"],
                )
                sent += 1
                logger.info(f"Dispatched job for {api['name']} (cursor={api.get('cursor')})")

            except Exception as e:
                logger.error(f"Failed to dispatch {api['name']}: {e}")
                failed += 1

        logger.info(f"Dispatch complete: {sent} sent, {failed} failed")
        return {"sent": sent, "failed": failed}

    @task
    def verify_dispatch(dispatch_result: dict):
        """
        Simple sanity check — alert if any jobs failed to dispatch.
        This is the ONLY thing Airflow knows about: did dispatch succeed?
        It does NOT know if Lambda succeeded — that's monitored separately.
        """
        if dispatch_result["failed"] > 0:
            raise ValueError(
                f"{dispatch_result['failed']} jobs failed to dispatch to SQS. "
                f"Check logs for details."
            )
        logger.info(
            f"All {dispatch_result['sent']} jobs dispatched successfully. "
            f"Lambda workers will now process them independently."
        )

    # ── Wire the tasks together ───────────────────────────────────────────────
    # Flow: load configs → load checkpoints → dispatch to SQS → verify
    # Total Airflow runtime: ~2-3 seconds for 50 APIs
    configs = load_api_configs()
    configs_with_cursors = load_checkpoints(configs)
    result = dispatch_to_sqs(configs_with_cursors)
    verify_dispatch(result)


dag = api_ingestion_dispatcher()
