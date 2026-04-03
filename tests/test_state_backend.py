#!/usr/bin/env python3
"""
Tests for the state_backend module.

Unit tests (mocked) + integration tests (real S3).
Run: python3 -m pytest test_state_backend.py -v
"""

import json
import os
import time
import uuid
from decimal import Decimal
from unittest.mock import MagicMock, patch

import boto3
import pytest
from botocore.exceptions import ClientError

# Ensure backend defaults to dynamodb for test isolation
os.environ.setdefault("STATE_BACKEND", "dynamodb")
os.environ.setdefault("SNS_TOPIC_ARN", "arn:aws:sns:us-east-1:123456789:test")

from state_backend import (
    ConditionalCheckFailedError,
    DynamoDBStateBackend,
    S3StateBackend,
    StateBackend,
    create_backend,
)


# ===========================================================================
# Unit Tests — S3 Backend (mocked)
# ===========================================================================


class TestS3StateBackendUnit:
    """Unit tests with mocked S3 client."""

    def _make_backend(self):
        backend = S3StateBackend.__new__(S3StateBackend)
        backend._s3 = MagicMock()
        backend._bucket = "test-bucket"
        backend._key = "failover-state/REGION_STATE.json"
        backend._region = "us-east-1"
        backend._last_etag = None
        return backend

    def test_get_state_returns_parsed_json(self):
        backend = self._make_backend()
        state = {"active_region": "us-east-1", "state": "PRIMARY_ACTIVE"}
        body_mock = MagicMock()
        body_mock.read.return_value = json.dumps(state).encode()
        backend._s3.get_object.return_value = {"Body": body_mock, "ETag": '"abc123"'}

        result = backend.get_state()
        assert result == state
        assert backend._last_etag == '"abc123"'

    def test_get_state_returns_empty_on_no_such_key(self):
        backend = self._make_backend()
        error = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "not found"}},
            "GetObject",
        )
        backend._s3.get_object.side_effect = error

        result = backend.get_state()
        assert result == {}

    def test_put_state_writes_json(self):
        backend = self._make_backend()
        backend._s3.put_object.return_value = {"ETag": '"new123"'}

        state = {"active_region": "us-east-1", "state": "PRIMARY_ACTIVE"}
        backend.put_state(state)

        call_kwargs = backend._s3.put_object.call_args[1]
        assert call_kwargs["Bucket"] == "test-bucket"
        written = json.loads(call_kwargs["Body"])
        assert written["active_region"] == "us-east-1"
        assert "pk" not in written  # pk stripped

    def test_put_state_strips_dynamodb_pk(self):
        backend = self._make_backend()
        backend._s3.put_object.return_value = {"ETag": '"new"'}

        state = {"pk": "REGION_STATE", "active_region": "us-east-1"}
        backend.put_state(state)

        body = json.loads(backend._s3.put_object.call_args[1]["Body"])
        assert "pk" not in body

    def test_update_state_read_modify_write(self):
        backend = self._make_backend()
        state = {"active_region": "us-east-1", "consecutive_failures": 0}

        body_mock = MagicMock()
        body_mock.read.return_value = json.dumps(state).encode()
        backend._s3.get_object.return_value = {"Body": body_mock, "ETag": '"etag1"'}
        backend._s3.put_object.return_value = {"ETag": '"etag2"'}

        backend.update_state({"consecutive_failures": 1})

        put_kwargs = backend._s3.put_object.call_args[1]
        assert put_kwargs["IfMatch"] == '"etag1"'
        written = json.loads(put_kwargs["Body"])
        assert written["consecutive_failures"] == 1
        assert written["active_region"] == "us-east-1"

    def test_conditional_update_succeeds_when_condition_met(self):
        backend = self._make_backend()
        state = {"state": "PRIMARY_ACTIVE", "consecutive_failures": 2}

        body_mock = MagicMock()
        body_mock.read.return_value = json.dumps(state).encode()
        backend._s3.get_object.return_value = {"Body": body_mock, "ETag": '"etag1"'}
        backend._s3.put_object.return_value = {"ETag": '"etag2"'}

        result = backend.conditional_update(
            condition_field="state",
            expected_value="PRIMARY_ACTIVE",
            updates={"state": "WAITING_AURORA_PROMOTION", "latch_engaged": True},
        )
        assert result is True

    def test_conditional_update_fails_when_condition_not_met(self):
        backend = self._make_backend()
        state = {"state": "SECONDARY_ACTIVE", "consecutive_failures": 0}

        body_mock = MagicMock()
        body_mock.read.return_value = json.dumps(state).encode()
        backend._s3.get_object.return_value = {"Body": body_mock, "ETag": '"etag1"'}

        result = backend.conditional_update(
            condition_field="state",
            expected_value="PRIMARY_ACTIVE",
            updates={"state": "WAITING_AURORA_PROMOTION"},
        )
        assert result is False
        # put_object should NOT have been called
        backend._s3.put_object.assert_not_called()

    def test_conditional_update_retries_on_etag_conflict(self):
        backend = self._make_backend()
        state = {"state": "PRIMARY_ACTIVE", "consecutive_failures": 2}

        body_mock = MagicMock()
        body_mock.read.return_value = json.dumps(state).encode()
        backend._s3.get_object.return_value = {"Body": body_mock, "ETag": '"etag1"'}

        # First put fails with 412, second succeeds
        error_412 = ClientError(
            {"Error": {"Code": "PreconditionFailed", "Message": "etag mismatch"}},
            "PutObject",
        )
        backend._s3.put_object.side_effect = [error_412, {"ETag": '"etag2"'}]

        result = backend.conditional_update(
            condition_field="state",
            expected_value="PRIMARY_ACTIVE",
            updates={"state": "WAITING_AURORA_PROMOTION"},
        )
        assert result is True
        assert backend._s3.put_object.call_count == 2

    def test_decimal_encoding(self):
        backend = self._make_backend()
        backend._s3.put_object.return_value = {"ETag": '"new"'}

        state = {"consecutive_failures": Decimal("3"), "cooldown_minutes": Decimal("30")}
        backend.put_state(state)

        body = json.loads(backend._s3.put_object.call_args[1]["Body"])
        assert body["consecutive_failures"] == 3
        assert isinstance(body["consecutive_failures"], int)


# ===========================================================================
# Unit Tests — Factory
# ===========================================================================


class TestFactory:
    def test_default_creates_dynamodb_backend(self):
        with patch.dict(os.environ, {"STATE_BACKEND": "dynamodb", "STATE_TABLE": "test-table"}):
            with patch("state_backend.boto3") as mock_boto3:
                mock_boto3.resource.return_value.Table.return_value = MagicMock()
                backend = create_backend(region="us-east-1")
                assert isinstance(backend, DynamoDBStateBackend)

    def test_s3_creates_s3_backend(self):
        with patch.dict(os.environ, {"STATE_BACKEND": "s3", "STATE_BUCKET": "my-bucket"}):
            with patch("state_backend.boto3") as mock_boto3:
                backend = create_backend(region="us-east-1")
                assert isinstance(backend, S3StateBackend)

    def test_s3_requires_bucket(self):
        with patch.dict(os.environ, {"STATE_BACKEND": "s3"}, clear=False):
            env = os.environ.copy()
            env.pop("STATE_BUCKET", None)
            with patch.dict(os.environ, env, clear=True):
                with pytest.raises(ValueError, match="STATE_BUCKET"):
                    create_backend(region="us-east-1")

    def test_unknown_backend_raises(self):
        with patch.dict(os.environ, {"STATE_BACKEND": "redis"}):
            with pytest.raises(ValueError, match="redis"):
                create_backend(region="us-east-1")


# ===========================================================================
# Integration Tests — Real S3 (requires AWS credentials)
# ===========================================================================

# Set INTEGRATION_TEST=1 to run these
INTEGRATION = os.environ.get("INTEGRATION_TEST", "0") == "1"
INTEGRATION_REGION = os.environ.get("INTEGRATION_REGION", "us-east-1")


def _unique_bucket_name():
    return f"fo-test-state-{uuid.uuid4().hex[:12]}"


@pytest.fixture
def s3_bucket():
    """Create a temporary S3 bucket for testing."""
    if not INTEGRATION:
        pytest.skip("Set INTEGRATION_TEST=1 to run S3 integration tests")

    bucket_name = _unique_bucket_name()
    s3 = boto3.client("s3", region_name=INTEGRATION_REGION)

    create_kwargs = {"Bucket": bucket_name}
    if INTEGRATION_REGION != "us-east-1":
        create_kwargs["CreateBucketConfiguration"] = {
            "LocationConstraint": INTEGRATION_REGION
        }
    s3.create_bucket(**create_kwargs)
    s3.put_bucket_versioning(
        Bucket=bucket_name,
        VersioningConfiguration={"Status": "Enabled"},
    )

    yield bucket_name

    # Cleanup
    s3_res = boto3.resource("s3", region_name=INTEGRATION_REGION)
    bucket_obj = s3_res.Bucket(bucket_name)
    bucket_obj.object_versions.all().delete()
    bucket_obj.delete()


@pytest.fixture
def s3_backend(s3_bucket):
    return S3StateBackend(
        bucket=s3_bucket,
        region=INTEGRATION_REGION,
        prefix="test-state/",
    )


class TestS3Integration:
    def test_get_state_empty(self, s3_backend):
        state = s3_backend.get_state()
        assert state == {}

    def test_put_and_get(self, s3_backend):
        state = {
            "active_region": "us-east-1",
            "state": "PRIMARY_ACTIVE",
            "latch_engaged": False,
            "consecutive_failures": 0,
        }
        s3_backend.put_state(state)
        result = s3_backend.get_state()
        assert result["active_region"] == "us-east-1"
        assert result["state"] == "PRIMARY_ACTIVE"
        assert result["latch_engaged"] is False

    def test_update_state(self, s3_backend):
        s3_backend.put_state({"active_region": "us-east-1", "consecutive_failures": 0})
        s3_backend.update_state({"consecutive_failures": 1})
        result = s3_backend.get_state()
        assert result["consecutive_failures"] == 1
        assert result["active_region"] == "us-east-1"

    def test_conditional_update_success(self, s3_backend):
        s3_backend.put_state({"state": "PRIMARY_ACTIVE", "latch_engaged": False})
        result = s3_backend.conditional_update(
            condition_field="state",
            expected_value="PRIMARY_ACTIVE",
            updates={"state": "WAITING_AURORA_PROMOTION", "latch_engaged": True},
        )
        assert result is True
        state = s3_backend.get_state()
        assert state["state"] == "WAITING_AURORA_PROMOTION"
        assert state["latch_engaged"] is True

    def test_conditional_update_failure(self, s3_backend):
        s3_backend.put_state({"state": "SECONDARY_ACTIVE", "latch_engaged": True})
        result = s3_backend.conditional_update(
            condition_field="state",
            expected_value="PRIMARY_ACTIVE",
            updates={"state": "WAITING_AURORA_PROMOTION"},
        )
        assert result is False
        state = s3_backend.get_state()
        assert state["state"] == "SECONDARY_ACTIVE"  # unchanged

    def test_conditional_increment(self, s3_backend):
        s3_backend.put_state({"consecutive_failures": 2, "state": "PRIMARY_ACTIVE"})

        # First increment should succeed
        result = s3_backend.conditional_update(
            condition_field="consecutive_failures",
            expected_value=2,
            updates={"consecutive_failures": 3},
        )
        assert result is True

        # Same expected value should now fail
        result = s3_backend.conditional_update(
            condition_field="consecutive_failures",
            expected_value=2,
            updates={"consecutive_failures": 3},
        )
        assert result is False

    def test_full_lifecycle(self, s3_backend):
        """Simulate: init → healthy → degraded → failover → failback."""
        # Init
        s3_backend.put_state({
            "active_region": "us-east-1",
            "state": "PRIMARY_ACTIVE",
            "latch_engaged": False,
            "consecutive_failures": 0,
            "last_failover_ts": "1970-01-01T00:00:00Z",
        })

        # Health check failures accumulate
        for i in range(3):
            s3_backend.conditional_update(
                "consecutive_failures", i, {"consecutive_failures": i + 1}
            )

        # Claim failover
        claimed = s3_backend.conditional_update(
            "state", "PRIMARY_ACTIVE",
            {
                "state": "WAITING_AURORA_PROMOTION",
                "active_region": "us-east-2",
                "latch_engaged": True,
                "consecutive_failures": 0,
            },
        )
        assert claimed is True

        state = s3_backend.get_state()
        assert state["state"] == "WAITING_AURORA_PROMOTION"
        assert state["active_region"] == "us-east-2"
        assert state["latch_engaged"] is True

        # Second claim should fail (already claimed)
        claimed2 = s3_backend.conditional_update(
            "state", "PRIMARY_ACTIVE",
            {"state": "WAITING_AURORA_PROMOTION"},
        )
        assert claimed2 is False

        # Failback
        s3_backend.update_state({
            "active_region": "us-east-1",
            "state": "PRIMARY_ACTIVE",
            "latch_engaged": False,
            "consecutive_failures": 0,
        })

        state = s3_backend.get_state()
        assert state["state"] == "PRIMARY_ACTIVE"
        assert state["latch_engaged"] is False


# ===========================================================================
# CRR Integration Test (requires two regions)
# ===========================================================================

CRR_TEST = os.environ.get("CRR_TEST", "0") == "1"
CRR_PRIMARY_REGION = os.environ.get("CRR_PRIMARY_REGION", "us-east-1")
CRR_SECONDARY_REGION = os.environ.get("CRR_SECONDARY_REGION", "us-east-2")


class TestCRRIntegration:
    """Test that state written in one region replicates to the other.

    Requires: CRR_TEST=1 and pre-provisioned buckets with CRR enabled.
    Set CRR_PRIMARY_BUCKET and CRR_SECONDARY_BUCKET env vars.
    """

    @pytest.fixture(autouse=True)
    def skip_unless_crr(self):
        if not CRR_TEST:
            pytest.skip("Set CRR_TEST=1 with CRR_PRIMARY_BUCKET and CRR_SECONDARY_BUCKET")

    def test_state_replicates_primary_to_secondary(self):
        primary_bucket = os.environ["CRR_PRIMARY_BUCKET"]
        secondary_bucket = os.environ["CRR_SECONDARY_BUCKET"]

        primary = S3StateBackend(bucket=primary_bucket, region=CRR_PRIMARY_REGION, prefix="failover-state/")
        secondary = S3StateBackend(bucket=secondary_bucket, region=CRR_SECONDARY_REGION, prefix="failover-state/")

        # Write to primary
        test_value = f"test-{uuid.uuid4().hex[:8]}"
        primary.put_state({"test_key": test_value, "state": "PRIMARY_ACTIVE"})

        # Poll secondary for up to 120s
        replicated = False
        for i in range(24):
            time.sleep(5)
            state = secondary.get_state()
            if state.get("test_key") == test_value:
                replicated = True
                print(f"  Replication took ~{(i+1)*5}s")
                break

        assert replicated, f"State did not replicate within 120s"

        # Cleanup
        s3 = boto3.client("s3", region_name=CRR_PRIMARY_REGION)
        s3.delete_object(Bucket=primary_bucket, Key="failover-state/REGION_STATE.json")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
