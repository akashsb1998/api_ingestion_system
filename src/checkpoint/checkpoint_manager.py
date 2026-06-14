"""
checkpoint_manager.py
---------------------
Saves and loads the cursor (bookmark) for each API.

WHY DO WE NEED THIS?
  Imagine API1 has 10,000 records. Lambda fetches page 1, page 2,
  page 3 ... and crashes on page 7. Without a checkpoint, the next
  run starts from page 1 again — duplicating records 1–600.

  With a checkpoint, the next run reads: "last time we got to page 6,
  cursor = eyJwYWdlIjo3fQ". So we resume from page 7.

  It's exactly like a video game save point.

WHERE IS IT STORED?
  DynamoDB — a simple key-value store on AWS.
  Key   = api_name  (e.g. "api1_fx_rates")
  Value = last cursor + metadata

  This is the same concept as the watermark table you use in
  Snowflake on the Convera project — just on AWS instead.
"""

import boto3
import logging
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class Checkpoint:
    """Represents where an API run left off."""
    api_name: str
    last_cursor: Optional[str]       # next_page_token or since_timestamp
    last_run_at: Optional[str]       # when the last successful run finished
    records_fetched: int = 0         # how many records last run got
    status: str = "success"          # success / failed


class CheckpointManager:
    """
    Read and write checkpoints to DynamoDB.

    DynamoDB table schema:
        PK: api_name (String)
        Attributes: last_cursor, last_run_at, records_fetched, status
    """

    def __init__(self, table_name: str, region: str = "ap-south-1"):
        self.table_name = table_name
        self.dynamodb = boto3.resource("dynamodb", region_name=region)
        self.table = self.dynamodb.Table(table_name)

    def load(self, api_name: str) -> Checkpoint:
        """
        Load the saved cursor for an API.

        If no checkpoint exists (first ever run), returns a Checkpoint
        with last_cursor=None — meaning "start from the beginning."
        """
        response = self.table.get_item(Key={"api_name": api_name})
        item = response.get("Item")

        if not item:
            logger.info(f"{api_name}: no checkpoint found — starting from scratch")
            return Checkpoint(api_name=api_name, last_cursor=None, last_run_at=None)

        checkpoint = Checkpoint(
            api_name=api_name,
            last_cursor=item.get("last_cursor"),
            last_run_at=item.get("last_run_at"),
            records_fetched=int(item.get("records_fetched", 0)),
            status=item.get("status", "success"),
        )
        logger.info(
            f"{api_name}: loaded checkpoint — cursor={checkpoint.last_cursor}, "
            f"last_run={checkpoint.last_run_at}"
        )
        return checkpoint

    def save(self, checkpoint: Checkpoint):
        """
        Save the cursor after a successful run.

        IMPORTANT: This is only called AFTER data is successfully
        written to S3. Never before. This guarantees that if Lambda
        crashes between the S3 write and this call, the next run
        will re-fetch and re-write — no data loss.
        """
        now = datetime.now(timezone.utc).isoformat()

        self.table.put_item(Item={
            "api_name": checkpoint.api_name,
            "last_cursor": checkpoint.last_cursor or "COMPLETED",
            "last_run_at": now,
            "records_fetched": checkpoint.records_fetched,
            "status": checkpoint.status,
            "updated_at": now,
        })
        logger.info(
            f"{checkpoint.api_name}: checkpoint saved — "
            f"cursor={checkpoint.last_cursor}, records={checkpoint.records_fetched}"
        )

    def mark_failed(self, api_name: str, error: str):
        """Mark an API run as failed without changing the cursor."""
        self.table.update_item(
            Key={"api_name": api_name},
            UpdateExpression="SET #s = :s, error_message = :e, updated_at = :t",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":s": "failed",
                ":e": error[:500],  # DynamoDB item size limit
                ":t": datetime.now(timezone.utc).isoformat(),
            },
        )
        logger.warning(f"{api_name}: marked as failed — {error}")
