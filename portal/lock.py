"""Test lock management via DynamoDB. Prevents concurrent tests."""

import time
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

from portal.config import STATE_TABLE, PRIMARY_REGION

TTL_SECONDS = 4 * 3600  # 4 hours auto-release


def _ddb():
    return boto3.client("dynamodb", region_name=PRIMARY_REGION)


def acquire_lock(operator, test_config):
    """Acquire the test lock. Returns True if acquired, False if held by someone else."""
    now = datetime.now(timezone.utc).isoformat()
    ttl = int(time.time()) + TTL_SECONDS

    try:
        _ddb().put_item(
            TableName=STATE_TABLE,
            Item={
                "pk": {"S": "PORTAL_LOCK"},
                "locked": {"BOOL": True},
                "locked_by": {"S": operator},
                "locked_at": {"S": now},
                "test_config": {"S": test_config},
                "ttl": {"N": str(ttl)},
            },
            ConditionExpression="attribute_not_exists(pk) OR locked = :f OR #t < :now",
            ExpressionAttributeNames={"#t": "ttl"},
            ExpressionAttributeValues={
                ":f": {"BOOL": False},
                ":now": {"N": str(int(time.time()))},
            },
        )
        return True
    except ClientError as e:
        if "ConditionalCheckFailedException" in str(e):
            return False
        raise


def release_lock():
    """Release the test lock."""
    _ddb().put_item(
        TableName=STATE_TABLE,
        Item={
            "pk": {"S": "PORTAL_LOCK"},
            "locked": {"BOOL": False},
            "locked_by": {"S": ""},
            "locked_at": {"S": ""},
            "test_config": {"S": ""},
            "ttl": {"N": "0"},
        },
    )


def get_lock_status():
    """Get current lock state. Returns dict with locked, locked_by, locked_at, test_config."""
    try:
        resp = _ddb().get_item(
            TableName=STATE_TABLE,
            Key={"pk": {"S": "PORTAL_LOCK"}},
            ConsistentRead=True,
        )
        item = resp.get("Item")
        if not item:
            return {"locked": False, "locked_by": "", "locked_at": "", "test_config": ""}

        locked = item.get("locked", {}).get("BOOL", False)
        ttl_val = int(item.get("ttl", {}).get("N", "0"))

        # Check TTL expiry
        if locked and ttl_val > 0 and ttl_val < int(time.time()):
            locked = False  # Expired

        return {
            "locked": locked,
            "locked_by": item.get("locked_by", {}).get("S", ""),
            "locked_at": item.get("locked_at", {}).get("S", ""),
            "test_config": item.get("test_config", {}).get("S", ""),
        }
    except ClientError:
        return {"locked": False, "locked_by": "", "locked_at": "", "test_config": ""}
