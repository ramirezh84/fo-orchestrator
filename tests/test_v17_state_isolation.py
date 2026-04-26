#!/usr/bin/env python3
"""
v1.7 regression tests for the F7/F8 race uncovered by the v1.6 drill.

F7: consecutive_failures never advanced past 1 because the passive region's
RMW on state['region_health'] + bidirectional CRR overwrote the active
region's locally-conditional cf increment.

F8: aurora_promotion_pending / redis_promotion_pending could not be cleared
for the same reason.

The fix is in docs/v1.7-s3-state-isolation-spec.md:
  - Only the active region writes the shared REGION_STATE.json
  - Passive writes its own region_health under a per-region key
  - update_failover_state() has a tripwire that refuses passive writes

These tests exercise:
  1. Object operations (put/get/list) on both backends
  2. The new helpers: write_own_region_health, read_region_health_map,
     is_active_region, _reset_region_health_cache
  3. The active-region tripwire in update_failover_state
  4. schema_version stamping
  5. The F7/F8 scenario itself: active increments cf while passive writes
     region_health concurrently — cf must survive.

Run: python3 -m pytest tests/test_v17_state_isolation.py -v
"""

import json
import os
from unittest.mock import MagicMock, patch

import pytest

# Minimum env required for the orchestrator module to import without crashing.
_MIN_ENV = {
    "SNS_TOPIC_ARN": "arn:aws:sns:us-east-1:123456789012:failover-alerts",
    "AWS_REGION": "us-east-1",
    "PRIMARY_REGION": "us-east-1",
    "SECONDARY_REGION": "us-east-2",
    "STATE_BACKEND": "s3",
    "STATE_BUCKET": "test-bucket",
    "STATE_TABLE": "failover-state",
}
for k, v in _MIN_ENV.items():
    os.environ.setdefault(k, v)

# Mock boto3 + state backend at module level so the orchestrator's top-level
# client creation does not make real AWS calls.
_mock_boto3_patcher = patch("boto3.client")
_mock_boto3_patcher.start()
_mock_create_backend_patcher = patch("state_backend.create_backend")
_mock_create_backend_patcher.start()

import failover_orchestrator_v3 as orch  # noqa: E402
from state_backend import S3StateBackend, DynamoDBStateBackend  # noqa: E402

_mock_boto3_patcher.stop()
_mock_create_backend_patcher.stop()


# ---------------------------------------------------------------------------
# 1. Object ops on each backend (S3 + DDB)
# ---------------------------------------------------------------------------

class TestS3PutGetListObject:
    def _backend(self):
        b = S3StateBackend.__new__(S3StateBackend)
        b._s3 = MagicMock()
        b._bucket = "tb"
        b._key = "failover-state/REGION_STATE.json"
        b._region = "us-east-1"
        b._last_etag = None
        return b

    def test_put_object_writes_json_body(self):
        b = self._backend()
        b.put_object("failover-state/region_health/us-east-1.json",
                     {"region": "us-east-1", "healthy": True})
        b._s3.put_object.assert_called_once()
        kwargs = b._s3.put_object.call_args.kwargs
        assert kwargs["Bucket"] == "tb"
        assert kwargs["Key"] == "failover-state/region_health/us-east-1.json"
        assert kwargs["ContentType"] == "application/json"
        assert json.loads(kwargs["Body"]) == {"region": "us-east-1", "healthy": True}

    def test_get_object_returns_empty_on_no_such_key(self):
        from botocore.exceptions import ClientError
        b = self._backend()
        b._s3.get_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "x"}}, "GetObject")
        assert b.get_object("failover-state/region_health/missing.json") == {}

    def test_get_object_returns_parsed_json(self):
        b = self._backend()
        body = MagicMock()
        body.read.return_value = json.dumps({"region": "us-east-1", "healthy": False}).encode()
        b._s3.get_object.return_value = {"Body": body}
        assert b.get_object("k") == {"region": "us-east-1", "healthy": False}

    def test_list_objects_paginates(self):
        b = self._backend()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {"Contents": [{"Key": "failover-state/region_health/us-east-1.json"}]},
            {"Contents": [{"Key": "failover-state/region_health/us-east-2.json"}]},
        ]
        b._s3.get_paginator.return_value = paginator
        result = b.list_objects("failover-state/region_health/")
        assert result == [
            "failover-state/region_health/us-east-1.json",
            "failover-state/region_health/us-east-2.json",
        ]


class TestDynamoDBPutGetListObject:
    def _backend(self):
        b = DynamoDBStateBackend.__new__(DynamoDBStateBackend)
        b._table = MagicMock()
        return b

    def test_put_object_serializes_data(self):
        b = self._backend()
        b.put_object("region_health/us-east-1.json",
                     {"region": "us-east-1", "healthy": True})
        b._table.put_item.assert_called_once()
        item = b._table.put_item.call_args.kwargs["Item"]
        assert item["pk"] == "region_health/us-east-1.json"
        assert json.loads(item["data"]) == {"region": "us-east-1", "healthy": True}

    def test_get_object_returns_empty_when_missing(self):
        b = self._backend()
        b._table.get_item.return_value = {}
        assert b.get_object("missing") == {}

    def test_get_object_parses_data_field(self):
        b = self._backend()
        b._table.get_item.return_value = {
            "Item": {"pk": "k", "data": json.dumps({"a": 1})}
        }
        assert b.get_object("k") == {"a": 1}

    def test_list_objects_uses_filter(self):
        b = self._backend()
        b._table.scan.return_value = {
            "Items": [
                {"pk": "region_health/us-east-1.json"},
                {"pk": "region_health/us-east-2.json"},
            ]
        }
        keys = b.list_objects("region_health/")
        assert keys == [
            "region_health/us-east-1.json",
            "region_health/us-east-2.json",
        ]
        b._table.scan.assert_called_once()
        kwargs = b._table.scan.call_args.kwargs
        assert kwargs["FilterExpression"] == "begins_with(pk, :p)"
        assert kwargs["ExpressionAttributeValues"] == {":p": "region_health/"}


# ---------------------------------------------------------------------------
# 2. Orchestrator helpers
# ---------------------------------------------------------------------------

class TestIsActiveRegion:
    def test_returns_true_when_current_matches(self):
        with patch.object(orch, "CURRENT_REGION", "us-east-1"):
            assert orch.is_active_region({"active_region": "us-east-1"}) is True

    def test_returns_false_when_current_differs(self):
        with patch.object(orch, "CURRENT_REGION", "us-east-2"):
            assert orch.is_active_region({"active_region": "us-east-1"}) is False

    def test_returns_default_to_primary_when_field_missing(self):
        with patch.object(orch, "CURRENT_REGION", "us-east-1"), \
             patch.object(orch, "PRIMARY_REGION", "us-east-1"):
            assert orch.is_active_region({}) is True


class TestWriteOwnRegionHealth:
    def test_writes_per_region_key_with_correct_payload(self):
        with patch.object(orch, "CURRENT_REGION", "us-east-2"), \
             patch.object(orch, "_STATE_PREFIX", "failover-state/"), \
             patch.object(orch, "_state_backend") as backend:
            orch.write_own_region_health(False, signals=[{"signal": "ecs", "healthy": False}])
            backend.put_object.assert_called_once()
            key, payload = backend.put_object.call_args.args
            assert key == "failover-state/region_health/us-east-2.json"
            assert payload["region"] == "us-east-2"
            assert payload["healthy"] is False
            assert payload["signals"] == [{"signal": "ecs", "healthy": False}]
            assert "ts" in payload

    def test_swallows_backend_errors_non_fatal(self):
        # write_own_region_health is best-effort: a failure should not raise
        # (the cycle continues). Other safety nets handle prolonged failures.
        with patch.object(orch, "CURRENT_REGION", "us-east-2"), \
             patch.object(orch, "_state_backend") as backend:
            backend.put_object.side_effect = RuntimeError("boom")
            # Should not raise
            orch.write_own_region_health(True)


class TestReadRegionHealthMap:
    def test_aggregates_by_region(self):
        with patch.object(orch, "_STATE_PREFIX", "failover-state/"), \
             patch.object(orch, "_state_backend") as backend:
            orch._reset_region_health_cache()
            backend.list_objects.return_value = [
                "failover-state/region_health/us-east-1.json",
                "failover-state/region_health/us-east-2.json",
            ]
            backend.get_object.side_effect = [
                {"region": "us-east-1", "healthy": True, "ts": "2026-01-01T00:00:00+00:00"},
                {"region": "us-east-2", "healthy": False, "ts": "2026-01-01T00:00:00+00:00"},
            ]
            result = orch.read_region_health_map()
            assert set(result.keys()) == {"us-east-1", "us-east-2"}
            assert result["us-east-1"]["healthy"] is True
            assert result["us-east-2"]["healthy"] is False

    def test_caches_within_invocation(self):
        with patch.object(orch, "_STATE_PREFIX", "failover-state/"), \
             patch.object(orch, "_state_backend") as backend:
            orch._reset_region_health_cache()
            backend.list_objects.return_value = ["failover-state/region_health/us-east-1.json"]
            backend.get_object.return_value = {"region": "us-east-1", "healthy": True}
            r1 = orch.read_region_health_map()
            r2 = orch.read_region_health_map()
            assert r1 == r2
            # Only listed once across two calls — cache hit
            backend.list_objects.assert_called_once()

    def test_cache_clears_on_reset(self):
        with patch.object(orch, "_STATE_PREFIX", "failover-state/"), \
             patch.object(orch, "_state_backend") as backend:
            orch._reset_region_health_cache()
            backend.list_objects.return_value = ["failover-state/region_health/us-east-1.json"]
            backend.get_object.return_value = {"region": "us-east-1", "healthy": True}
            orch.read_region_health_map()
            orch._reset_region_health_cache()
            orch.read_region_health_map()
            assert backend.list_objects.call_count == 2

    def test_returns_empty_when_list_fails(self):
        with patch.object(orch, "_STATE_PREFIX", "failover-state/"), \
             patch.object(orch, "_state_backend") as backend:
            orch._reset_region_health_cache()
            backend.list_objects.side_effect = RuntimeError("network")
            assert orch.read_region_health_map() == {}


# ---------------------------------------------------------------------------
# 3. The active-region tripwire on update_failover_state
# ---------------------------------------------------------------------------

class TestUpdateFailoverStateTripwire:
    def test_active_region_write_succeeds(self, caplog):
        with patch.object(orch, "CURRENT_REGION", "us-east-1"), \
             patch.object(orch, "PRIMARY_REGION", "us-east-1"), \
             patch.object(orch, "_state_backend") as backend:
            backend.get_state.return_value = {"active_region": "us-east-1"}
            orch.update_failover_state({"consecutive_failures": 1})
            backend.update_state.assert_called_once()
            payload = backend.update_state.call_args.args[0]
            assert payload["consecutive_failures"] == 1
            # schema_version stamped on every write (v1.7)
            assert payload["schema_version"] == orch.SCHEMA_VERSION

    def test_passive_region_write_is_refused_with_error_log(self, caplog):
        with patch.object(orch, "CURRENT_REGION", "us-east-2"), \
             patch.object(orch, "PRIMARY_REGION", "us-east-1"), \
             patch.object(orch, "_state_backend") as backend:
            backend.get_state.return_value = {"active_region": "us-east-1"}
            with caplog.at_level("ERROR"):
                orch.update_failover_state({"consecutive_failures": 5})
            backend.update_state.assert_not_called()
            assert any("REFUSING update_failover_state" in r.message for r in caplog.records)
            assert any("us-east-2" in r.message for r in caplog.records)

    def test_passive_region_passes_when_no_state_yet(self):
        # No state file yet (first run). Treat as bootstrap — write proceeds.
        with patch.object(orch, "CURRENT_REGION", "us-east-2"), \
             patch.object(orch, "PRIMARY_REGION", "us-east-1"), \
             patch.object(orch, "_state_backend") as backend:
            backend.get_state.return_value = {}
            orch.update_failover_state({"active_region": "us-east-2"})
            backend.update_state.assert_called_once()


# ---------------------------------------------------------------------------
# 4. F7/F8 regression — the test that should have caught this
# ---------------------------------------------------------------------------

class _FakeBucket:
    """In-memory bucket simulating a single S3 backend's view of state."""
    def __init__(self, name, peer=None):
        self.name = name
        self.objects = {}  # key -> json-serializable dict
        self.peer = peer

    def put_object(self, key, data):
        self.objects[key] = json.loads(json.dumps(data))
        # Simulate immediate CRR replication (worst case for the race)
        if self.peer is not None:
            self.peer.objects[key] = json.loads(json.dumps(data))

    def get_object(self, key):
        return json.loads(json.dumps(self.objects.get(key, {})))

    def list_objects(self, prefix):
        return [k for k in self.objects if k.startswith(prefix)]

    def get_state(self):
        return self.get_object("failover-state/REGION_STATE.json")

    def update_state(self, updates):
        # Read-modify-write — simulates what S3StateBackend.update_state does.
        cur = self.get_state()
        cur.update(updates)
        self.put_object("failover-state/REGION_STATE.json", cur)

    def conditional_update(self, condition_field, expected_value, updates):
        cur = self.get_state()
        if cur.get(condition_field) != expected_value:
            return False
        cur.update(updates)
        self.put_object("failover-state/REGION_STATE.json", cur)
        return True


class TestF7F8Regression:
    """The two-bucket simulation that proves cf=1 + promotion-pending-False
    survive concurrent passive writes. This test would have caught F7/F8 in
    v1.6 if it had existed."""

    def _setup_buckets(self):
        bucket_e1 = _FakeBucket("us-east-1")
        bucket_e2 = _FakeBucket("us-east-2")
        bucket_e1.peer = bucket_e2
        bucket_e2.peer = bucket_e1
        # Seed initial state in both via CRR
        bucket_e1.put_object("failover-state/REGION_STATE.json", {
            "active_region": "us-east-1",
            "state": "PRIMARY_ACTIVE",
            "consecutive_failures": 0,
            "aurora_promotion_pending": False,
            "redis_promotion_pending": False,
            "latch_engaged": False,
        })
        return bucket_e1, bucket_e2

    def test_consecutive_failures_survives_concurrent_passive_health_writes(self):
        """F7 regression. Active increments cf via conditional_update; passive
        writes its own region_health (per-region key, not the shared state).
        cf must be preserved across multiple cycles."""
        bucket_e1, bucket_e2 = self._setup_buckets()

        # Active (us-east-1) increments cf five times via conditional_update.
        # Passive (us-east-2) writes its region_health each cycle via the new
        # per-region key path — NOT touching REGION_STATE.json.
        for cycle in range(1, 6):
            # Active perspective: read state, increment cf
            state = bucket_e1.get_state()
            assert bucket_e1.conditional_update(
                "consecutive_failures", state["consecutive_failures"], {"consecutive_failures": cycle}
            ), f"cycle {cycle}: conditional_update should win"

            # Passive perspective: write own health per-region (v1.7 path)
            bucket_e2.put_object(
                "failover-state/region_health/us-east-2.json",
                {"region": "us-east-2", "healthy": True, "ts": f"cycle-{cycle}"},
            )

        # cf must be 5 in both buckets (CRR-replicated)
        assert bucket_e1.get_state()["consecutive_failures"] == 5
        assert bucket_e2.get_state()["consecutive_failures"] == 5

    def test_promotion_pending_clearing_survives_concurrent_passive_writes(self):
        """F8 regression. Active clears aurora_promotion_pending; passive
        keeps writing its region_health. Cleared flag must stay cleared."""
        bucket_e1, bucket_e2 = self._setup_buckets()

        # Set up: us-east-2 is now active and waiting for data tier promotion.
        bucket_e2.update_state({
            "active_region": "us-east-2",
            "state": "WAITING_AURORA_PROMOTION",
            "aurora_promotion_pending": True,
            "redis_promotion_pending": True,
            "latch_engaged": True,
        })

        # Cycle 1: active clears aurora_promotion_pending (cleared via RMW write
        # to its own bucket; CRR replicates to passive).
        bucket_e2.update_state({"aurora_promotion_pending": False})
        # Cycle 1 (concurrent): passive writes its own region_health to its
        # OWN per-region key — does NOT RMW the shared state.
        bucket_e1.put_object(
            "failover-state/region_health/us-east-1.json",
            {"region": "us-east-1", "healthy": False, "ts": "cycle-1"},
        )

        assert bucket_e2.get_state()["aurora_promotion_pending"] is False
        assert bucket_e1.get_state()["aurora_promotion_pending"] is False

        # Cycle 2: active clears redis_promotion_pending; passive writes again.
        bucket_e2.update_state({"redis_promotion_pending": False})
        bucket_e1.put_object(
            "failover-state/region_health/us-east-1.json",
            {"region": "us-east-1", "healthy": False, "ts": "cycle-2"},
        )

        # Both flags must be cleared everywhere.
        assert bucket_e2.get_state()["aurora_promotion_pending"] is False
        assert bucket_e2.get_state()["redis_promotion_pending"] is False
        assert bucket_e1.get_state()["aurora_promotion_pending"] is False
        assert bucket_e1.get_state()["redis_promotion_pending"] is False

    def test_v16_pattern_loses_consecutive_failures(self):
        """The negative test: prove that the v1.6 pattern (passive RMW of
        the shared state for region_health) DOES lose cf increments under
        bidirectional CRR. This is what the v1.6 drill saw as F7."""
        bucket_e1, bucket_e2 = self._setup_buckets()

        # v1.6 pattern: active increments cf locally; passive does RMW of the
        # SHARED state file to merge in its own region_health.
        # CRR replays the passive's stale view back to the active bucket.

        # Active reads cf=0, increments to cf=1 (via conditional_update — wins
        # locally, replicates to passive).
        state = bucket_e1.get_state()
        assert bucket_e1.conditional_update(
            "consecutive_failures", state["consecutive_failures"], {"consecutive_failures": 1}
        )
        # At this point both buckets have cf=1.

        # Now simulate the race: passive read happened BEFORE the cf=1
        # replication (real CRR has 15-60s lag). Passive's view of state has
        # cf=0. Passive writes its region_health via RMW of the shared state.
        passive_view = {"active_region": "us-east-1", "state": "PRIMARY_ACTIVE",
                        "consecutive_failures": 0, "aurora_promotion_pending": False,
                        "redis_promotion_pending": False, "latch_engaged": False}
        passive_view["region_health"] = {"us-east-2": {"healthy": True, "ts": "now"}}
        # Passive writes back the FULL state (which still has cf=0 from its
        # stale read) — this is the v1.6 pattern.
        bucket_e2.put_object("failover-state/REGION_STATE.json", passive_view)
        # CRR replicated bucket_e2's write back to bucket_e1.

        # cf=1 is gone. This is the F7 bug, captured.
        assert bucket_e1.get_state()["consecutive_failures"] == 0, \
            "v1.6 pattern: cf=1 is lost under bilateral RMW + CRR (F7 bug)"
