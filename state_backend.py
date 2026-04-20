"""
State Backend Abstraction for Failover Orchestrator.

Provides two implementations:
  - DynamoDBStateBackend: Original DynamoDB Global Table backend
  - S3StateBackend: S3 Cross-Region Replication backend (no DynamoDB required)

Select backend via STATE_BACKEND environment variable ("dynamodb" or "s3").
"""

import json
import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional, Tuple

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------------
STATE_KEY = "REGION_STATE"
DEFAULT_STATE_FIELDS = {
    "active_region": None,  # Must be set by caller
    "state": "PRIMARY_ACTIVE",
    "last_failover_ts": "1970-01-01T00:00:00Z",
    "cooldown_minutes": 30,
    "initiated_by": "INIT",
    "reason": "Initial state",
    "latch_engaged": False,
    "consecutive_failures": 0,
    "last_active_metric_ts": None,  # Must be set by caller
    "aurora_promotion_pending": False,
    "redis_promotion_pending": False,
    "region_health": {},  # Map of region -> {"healthy": bool, "ts": iso_str}
    "last_warning_notification_ts": "1970-01-01T00:00:00Z",
}


class ConditionalCheckFailedError(Exception):
    """Raised when a conditional write fails (equivalent to DynamoDB ConditionalCheckFailedException)."""
    pass


# ===========================================================================
# Abstract Base
# ===========================================================================

class StateBackend(ABC):
    """Interface for failover state storage."""

    @abstractmethod
    def get_state(self) -> dict:
        """Read current failover state. Returns empty dict if not found."""
        ...

    @abstractmethod
    def put_state(self, state: dict) -> None:
        """Overwrite the entire state (used for init and reset)."""
        ...

    @abstractmethod
    def update_state(self, updates: dict) -> None:
        """Merge *updates* into the current state."""
        ...

    @abstractmethod
    def conditional_update(self, condition_field: str, expected_value, updates: dict) -> bool:
        """
        Update state only if *condition_field* currently equals *expected_value*.

        Returns True if the write succeeded, False if the condition was not met
        (equivalent to DynamoDB ConditionalCheckFailedException).
        Raises on any other error.
        """
        ...


# ===========================================================================
# DynamoDB Backend (original)
# ===========================================================================

class DynamoDBStateBackend(StateBackend):
    """State storage using DynamoDB Global Table (original implementation)."""

    def __init__(self, table_name: str, region: str, client_config: Optional[BotoConfig] = None):
        cfg = client_config or BotoConfig(
            connect_timeout=10, read_timeout=30, retries={"max_attempts": 2}
        )
        dynamodb = boto3.resource("dynamodb", region_name=region, config=cfg)
        self._table = dynamodb.Table(table_name)
        logger.info(f"DynamoDBStateBackend initialized: table={table_name}, region={region}")

    def get_state(self) -> dict:
        try:
            response = self._table.get_item(Key={"pk": STATE_KEY})
            return response.get("Item", {})
        except ClientError as e:
            logger.error(f"DynamoDB GetItem failed: {e}")
            raise

    def put_state(self, state: dict) -> None:
        state["pk"] = STATE_KEY
        try:
            self._table.put_item(Item=state)
        except ClientError as e:
            logger.error(f"DynamoDB PutItem failed: {e}")
            raise

    def update_state(self, updates: dict) -> None:
        expression_parts = []
        expression_values = {}
        expression_names = {}
        for key, value in updates.items():
            safe_key = f"#k_{key}"
            safe_val = f":v_{key}"
            expression_parts.append(f"{safe_key} = {safe_val}")
            expression_names[safe_key] = key
            expression_values[safe_val] = value
        try:
            self._table.update_item(
                Key={"pk": STATE_KEY},
                UpdateExpression="SET " + ", ".join(expression_parts),
                ExpressionAttributeNames=expression_names,
                ExpressionAttributeValues=expression_values,
            )
        except ClientError as e:
            logger.error(f"DynamoDB UpdateItem failed: {e}")
            raise

    def conditional_update(self, condition_field: str, expected_value, updates: dict) -> bool:
        expression_parts = []
        expression_values = {":expected_val": expected_value}
        expression_names = {f"#cond": condition_field}

        for key, value in updates.items():
            safe_key = f"#k_{key}"
            safe_val = f":v_{key}"
            expression_parts.append(f"{safe_key} = {safe_val}")
            expression_names[safe_key] = key
            expression_values[safe_val] = value

        try:
            self._table.update_item(
                Key={"pk": STATE_KEY},
                UpdateExpression="SET " + ", ".join(expression_parts),
                ConditionExpression="#cond = :expected_val",
                ExpressionAttributeNames=expression_names,
                ExpressionAttributeValues=expression_values,
            )
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return False
            raise


# ===========================================================================
# S3 Backend (CRR-based alternative)
# ===========================================================================

class _StateEncoder(json.JSONEncoder):
    """Handle Decimal and datetime values in state JSON."""
    def default(self, o):
        if isinstance(o, Decimal):
            return int(o) if o == int(o) else float(o)
        if isinstance(o, datetime):
            return o.isoformat()
        return str(o)


class S3StateBackend(StateBackend):
    """
    State storage using S3 with Cross-Region Replication.

    State is stored as a JSON object at ``s3://<bucket>/<prefix>REGION_STATE.json``.
    Optimistic concurrency control uses S3 ETags (If-Match on PutObject) to
    provide conditional-write semantics equivalent to DynamoDB ConditionExpressions.

    Requires S3 versioning enabled (mandatory for CRR anyway).
    """

    def __init__(
        self,
        bucket: str,
        region: str,
        prefix: str = "failover-state/",
        client_config: Optional[BotoConfig] = None,
    ):
        cfg = client_config or BotoConfig(
            connect_timeout=10, read_timeout=30, retries={"max_attempts": 2}
        )
        self._s3 = boto3.client("s3", region_name=region, config=cfg)
        self._bucket = bucket
        self._key = f"{prefix}{STATE_KEY}.json"
        self._region = region
        # Cached ETag from last read — used for optimistic locking
        self._last_etag = None
        logger.info(
            f"S3StateBackend initialized: bucket={bucket}, key={self._key}, region={region}"
        )

    # -- helpers ----------------------------------------------------------

    def _read_object(self) -> Tuple[dict, Optional[str]]:
        """Return (state_dict, etag). Returns ({}, None) if object doesn't exist."""
        try:
            resp = self._s3.get_object(Bucket=self._bucket, Key=self._key)
            body = resp["Body"].read()
            etag = resp.get("ETag")
            state = json.loads(body)
            self._last_etag = etag
            return state, etag
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                self._last_etag = None
                return {}, None
            raise

    def _write_object(self, state: dict, if_match: Optional[str] = None) -> str:
        """
        Write state JSON to S3. Returns new ETag.

        If *if_match* is provided, uses conditional write (S3 If-Match header).
        Raises ConditionalCheckFailedError if the ETag doesn't match (412).
        """
        body = json.dumps(state, cls=_StateEncoder).encode("utf-8")
        kwargs = {
            "Bucket": self._bucket,
            "Key": self._key,
            "Body": body,
            "ContentType": "application/json",
        }
        if if_match:
            kwargs["IfMatch"] = if_match
        try:
            resp = self._s3.put_object(**kwargs)
            self._last_etag = resp.get("ETag")
            return self._last_etag
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code in ("PreconditionFailed", "412"):
                raise ConditionalCheckFailedError(
                    "S3 ETag mismatch — state was modified by another writer"
                )
            raise

    # -- public interface -------------------------------------------------

    def get_state(self) -> dict:
        state, _ = self._read_object()
        return state

    def put_state(self, state: dict) -> None:
        # Full overwrite — no conditional check
        state.pop("pk", None)  # Strip DynamoDB partition key if present
        self._write_object(state)
        logger.info("S3 state written (full overwrite)")

    def update_state(self, updates: dict) -> None:
        """Read-modify-write with ETag-based optimistic locking (auto-retry)."""
        for attempt in range(3):
            state, etag = self._read_object()
            if not state:
                # Object doesn't exist yet — create it
                state = {}
            state.update(updates)
            state.pop("pk", None)
            try:
                if etag:
                    self._write_object(state, if_match=etag)
                else:
                    self._write_object(state)
                return
            except ConditionalCheckFailedError:
                logger.warning(
                    f"S3 optimistic lock conflict on update_state (attempt {attempt + 1}/3), retrying"
                )
        # Final attempt without condition to avoid infinite retry
        state, _ = self._read_object()
        state.update(updates)
        state.pop("pk", None)
        self._write_object(state)

    def conditional_update(self, condition_field: str, expected_value, updates: dict) -> bool:
        """
        Read state, verify condition_field == expected_value, then write with ETag lock.

        Returns True if the condition was met and write succeeded.
        Returns False if the condition field didn't match (business logic failure).
        Retries once on ETag conflict (concurrent write from same region).
        """
        for attempt in range(2):
            state, etag = self._read_object()
            if not state:
                logger.warning("conditional_update: no state exists yet")
                return False

            # Check the business-logic condition
            current_value = state.get(condition_field)
            # Handle Decimal/int comparison
            if isinstance(current_value, (int, float)) and isinstance(expected_value, (int, float)):
                condition_met = current_value == expected_value
            else:
                condition_met = current_value == expected_value

            if not condition_met:
                logger.warning(
                    f"Conditional check failed: {condition_field}={current_value!r}, "
                    f"expected={expected_value!r}"
                )
                return False

            # Condition met — apply updates and try to write
            state.update(updates)
            state.pop("pk", None)
            try:
                self._write_object(state, if_match=etag)
                return True
            except ConditionalCheckFailedError:
                logger.warning(
                    f"S3 ETag conflict during conditional_update (attempt {attempt + 1}/2), retrying"
                )

        # Exhausted retries — treat as lost race
        logger.warning("conditional_update: exhausted retries due to ETag conflicts")
        return False


# ===========================================================================
# Factory
# ===========================================================================

def create_backend(
    region: Optional[str] = None,
    table_name: Optional[str] = None,
    bucket: Optional[str] = None,
    prefix: Optional[str] = None,
    client_config: Optional[BotoConfig] = None,
) -> StateBackend:
    """
    Create a state backend based on STATE_BACKEND environment variable.

    STATE_BACKEND = "dynamodb" (default) → DynamoDBStateBackend
    STATE_BACKEND = "s3"                 → S3StateBackend

    Required env vars per backend:
      dynamodb: STATE_TABLE (default "failover-state")
      s3:       STATE_BUCKET (required), STATE_PREFIX (default "failover-state/")
    """
    backend_type = os.environ.get("STATE_BACKEND", "dynamodb").lower()
    region = region or os.environ.get("AWS_REGION", "us-east-1")

    if backend_type == "s3":
        bucket = bucket or os.environ.get("STATE_BUCKET")
        if not bucket:
            raise ValueError(
                "STATE_BUCKET environment variable is required when STATE_BACKEND=s3"
            )
        prefix = prefix or os.environ.get("STATE_PREFIX", "failover-state/")
        return S3StateBackend(
            bucket=bucket, region=region, prefix=prefix, client_config=client_config,
        )
    elif backend_type == "dynamodb":
        table = table_name or os.environ.get("STATE_TABLE", "failover-state")
        return DynamoDBStateBackend(
            table_name=table, region=region, client_config=client_config,
        )
    else:
        raise ValueError(
            f"Unknown STATE_BACKEND={backend_type!r}. Must be 'dynamodb' or 's3'."
        )
