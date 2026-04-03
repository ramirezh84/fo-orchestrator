#!/usr/bin/env python3
"""
End-to-End Tests: Full Failover Lifecycle on S3 Backend
==========================================================
Tests every orchestrator scenario against a real S3 bucket, exercising
the same state-management functions the Lambda uses.

Scenarios tested:
  1. Initialization — first invocation creates default state
  2. Healthy active region — consecutive failures stay 0
  3. Degrading active region — failures increment with conditional writes
  4. Failover threshold reached — try_claim_failover succeeds, latch engages
  5. Concurrent failover race — second claim loses the race
  6. Latch enforcement — passive region blocked from publishing healthy
  7. Aurora promotion pending → confirmed
  8. Failback — latch released, state returns to PRIMARY_ACTIVE
  9. Cooldown enforcement — failover blocked during cooldown window
  10. State reset — operator resets to PRIMARY_ACTIVE
  11. Manual failover mode — state transitions correctly
  12. Region-level failure (passive detects stale active) — passive claims failover
  13. Cross-region read consistency — secondary reads replicated state

Run:
  INTEGRATION_TEST=1 python3 -m pytest test_e2e_s3_backend.py -v -s
"""

import json
import os
import time
import uuid
from datetime import datetime, timedelta, timezone

import boto3
import pytest

os.environ.setdefault("STATE_BACKEND", "s3")
os.environ.setdefault("SNS_TOPIC_ARN", "arn:aws:sns:us-east-1:123456789:test")

from state_backend import S3StateBackend, create_backend

INTEGRATION = os.environ.get("INTEGRATION_TEST", "0") == "1"
REGION = os.environ.get("INTEGRATION_REGION", "us-east-1")
PRIMARY_REGION = "us-east-1"
SECONDARY_REGION = "us-east-2"
COOLDOWN_MINUTES = 30
CONSECUTIVE_FAILURES_THRESHOLD = 3


def _unique_bucket():
    return f"fo-e2e-test-{uuid.uuid4().hex[:12]}"


@pytest.fixture(scope="module")
def s3_bucket():
    """Create a temporary versioned S3 bucket for the entire test module."""
    if not INTEGRATION:
        pytest.skip("Set INTEGRATION_TEST=1 to run E2E tests")

    bucket_name = _unique_bucket()
    s3 = boto3.client("s3", region_name=REGION)

    create_kwargs = {"Bucket": bucket_name}
    if REGION != "us-east-1":
        create_kwargs["CreateBucketConfiguration"] = {"LocationConstraint": REGION}
    s3.create_bucket(**create_kwargs)
    s3.put_bucket_versioning(
        Bucket=bucket_name, VersioningConfiguration={"Status": "Enabled"},
    )
    print(f"\n  [setup] Created test bucket: {bucket_name}")

    yield bucket_name

    # Teardown
    s3_res = boto3.resource("s3", region_name=REGION)
    s3_res.Bucket(bucket_name).object_versions.all().delete()
    s3_res.Bucket(bucket_name).delete()
    print(f"  [teardown] Deleted test bucket: {bucket_name}")


@pytest.fixture
def backend(s3_bucket):
    """Fresh S3 backend for each test (unique prefix to isolate)."""
    prefix = f"test-{uuid.uuid4().hex[:8]}/"
    return S3StateBackend(bucket=s3_bucket, region=REGION, prefix=prefix)


def _default_state(active_region=PRIMARY_REGION):
    """Matches the orchestrator's initial state."""
    return {
        "active_region": active_region,
        "state": "PRIMARY_ACTIVE" if active_region == PRIMARY_REGION else "SECONDARY_ACTIVE",
        "last_failover_ts": "1970-01-01T00:00:00Z",
        "cooldown_minutes": COOLDOWN_MINUTES,
        "initiated_by": "INIT",
        "reason": "Initial state",
        "latch_engaged": False,
        "consecutive_failures": 0,
        "last_active_metric_ts": datetime.now(timezone.utc).isoformat(),
        "aurora_promotion_pending": False,
        "last_warning_notification_ts": "1970-01-01T00:00:00Z",
    }


# ===========================================================================
# Scenario 1: Initialization
# ===========================================================================

class TestScenario01_Initialization:
    def test_first_read_returns_empty(self, backend):
        """First read on empty state returns {}."""
        state = backend.get_state()
        assert state == {}

    def test_first_write_creates_default_state(self, backend):
        """Orchestrator creates default state on first invocation."""
        state = backend.get_state()
        if not state:
            default = _default_state()
            backend.put_state(default)

        state = backend.get_state()
        assert state["active_region"] == PRIMARY_REGION
        assert state["state"] == "PRIMARY_ACTIVE"
        assert state["latch_engaged"] is False
        assert state["consecutive_failures"] == 0
        assert state["aurora_promotion_pending"] is False


# ===========================================================================
# Scenario 2: Healthy active region
# ===========================================================================

class TestScenario02_HealthyActive:
    def test_heartbeat_updates(self, backend):
        """Active region writes heartbeat on every invocation."""
        backend.put_state(_default_state())

        # Simulate 3 healthy invocations — heartbeat updates, failures stay 0
        for _ in range(3):
            now = datetime.now(timezone.utc).isoformat()
            backend.update_state({
                "last_active_metric_ts": now,
                "consecutive_failures": 0,
            })

        state = backend.get_state()
        assert state["consecutive_failures"] == 0
        assert state["state"] == "PRIMARY_ACTIVE"

    def test_failures_reset_on_recovery(self, backend):
        """If region was degrading but recovers, failures reset to 0."""
        init = _default_state()
        init["consecutive_failures"] = 2
        backend.put_state(init)

        # Healthy check resets failures
        backend.update_state({"consecutive_failures": 0})
        state = backend.get_state()
        assert state["consecutive_failures"] == 0


# ===========================================================================
# Scenario 3: Degrading active region
# ===========================================================================

class TestScenario03_DegradingActive:
    def test_consecutive_failures_increment(self, backend):
        """Failures increment with conditional writes (like try_increment_failures)."""
        backend.put_state(_default_state())

        for i in range(CONSECUTIVE_FAILURES_THRESHOLD):
            result = backend.conditional_update(
                condition_field="consecutive_failures",
                expected_value=i,
                updates={"consecutive_failures": i + 1},
            )
            assert result is True, f"Increment {i} -> {i+1} should succeed"

        state = backend.get_state()
        assert state["consecutive_failures"] == CONSECUTIVE_FAILURES_THRESHOLD

    def test_concurrent_increment_loses_race(self, backend):
        """Second increment with stale expected value fails."""
        backend.put_state(_default_state())

        # First invocation increments 0 → 1
        r1 = backend.conditional_update("consecutive_failures", 0, {"consecutive_failures": 1})
        assert r1 is True

        # Second invocation also tries 0 → 1 (stale read)
        r2 = backend.conditional_update("consecutive_failures", 0, {"consecutive_failures": 1})
        assert r2 is False

        state = backend.get_state()
        assert state["consecutive_failures"] == 1  # only incremented once


# ===========================================================================
# Scenario 4: Failover threshold reached
# ===========================================================================

class TestScenario04_FailoverExecution:
    def test_active_region_triggers_failover(self, backend):
        """When threshold reached, active region claims failover via conditional write."""
        init = _default_state()
        init["consecutive_failures"] = CONSECUTIVE_FAILURES_THRESHOLD
        backend.put_state(init)

        now = datetime.now(timezone.utc)
        target_region = SECONDARY_REGION

        claimed = backend.conditional_update(
            condition_field="state",
            expected_value="PRIMARY_ACTIVE",
            updates={
                "state": "WAITING_AURORA_PROMOTION",
                "active_region": target_region,
                "last_failover_ts": now.isoformat(),
                "latch_engaged": True,
                "consecutive_failures": 0,
                "initiated_by": "AUTO_ACTIVE",
                "reason": "Health check failures exceeded threshold",
                "aurora_promotion_pending": True,
            },
        )
        assert claimed is True

        state = backend.get_state()
        assert state["state"] == "WAITING_AURORA_PROMOTION"
        assert state["active_region"] == SECONDARY_REGION
        assert state["latch_engaged"] is True
        assert state["consecutive_failures"] == 0
        assert state["aurora_promotion_pending"] is True
        assert state["initiated_by"] == "AUTO_ACTIVE"


# ===========================================================================
# Scenario 5: Concurrent failover race
# ===========================================================================

class TestScenario05_ConcurrentFailoverRace:
    def test_second_claim_loses(self, backend):
        """Only one invocation can claim failover — second gets False."""
        init = _default_state()
        init["consecutive_failures"] = CONSECUTIVE_FAILURES_THRESHOLD
        backend.put_state(init)

        now = datetime.now(timezone.utc)

        # First invocation claims
        claim1 = backend.conditional_update(
            "state", "PRIMARY_ACTIVE",
            {
                "state": "WAITING_AURORA_PROMOTION",
                "active_region": SECONDARY_REGION,
                "latch_engaged": True,
                "last_failover_ts": now.isoformat(),
                "initiated_by": "AUTO_ACTIVE",
            },
        )
        assert claim1 is True

        # Second invocation tries the same claim (stale state)
        claim2 = backend.conditional_update(
            "state", "PRIMARY_ACTIVE",
            {
                "state": "WAITING_AURORA_PROMOTION",
                "active_region": SECONDARY_REGION,
                "latch_engaged": True,
            },
        )
        assert claim2 is False

        # State is from first claimant only
        state = backend.get_state()
        assert state["state"] == "WAITING_AURORA_PROMOTION"
        assert state["initiated_by"] == "AUTO_ACTIVE"


# ===========================================================================
# Scenario 6: Latch enforcement
# ===========================================================================

class TestScenario06_LatchEnforcement:
    def test_passive_region_blocked_by_latch(self, backend):
        """After failover, old region (now passive) sees latch=True and must publish 0."""
        # Post-failover state: us-east-2 is active, latch engaged on us-east-1
        backend.put_state({
            "active_region": SECONDARY_REGION,
            "state": "WAITING_AURORA_PROMOTION",
            "latch_engaged": True,
            "consecutive_failures": 0,
            "last_failover_ts": datetime.now(timezone.utc).isoformat(),
            "aurora_promotion_pending": True,
            "initiated_by": "AUTO_ACTIVE",
            "reason": "Test failover",
        })

        # Passive region (us-east-1) reads state
        state = backend.get_state()
        passive_region = PRIMARY_REGION  # old active is now passive
        active_region = state["active_region"]

        # Simulate passive handler latch check
        assert passive_region != active_region, "Should be passive"
        assert state["latch_engaged"] is True
        # Passive handler would publish 0 and return immediately
        # The latch prevents traffic from routing back

    def test_latch_survives_multiple_reads(self, backend):
        """Latch persists across multiple read cycles (not auto-cleared)."""
        backend.put_state({
            "active_region": SECONDARY_REGION,
            "state": "WAITING_AURORA_PROMOTION",
            "latch_engaged": True,
            "consecutive_failures": 0,
            "aurora_promotion_pending": True,
        })

        # 5 simulated 1-minute invocations — latch stays
        for _ in range(5):
            state = backend.get_state()
            assert state["latch_engaged"] is True, "Latch must not auto-clear"


# ===========================================================================
# Scenario 7: Aurora promotion pending → confirmed
# ===========================================================================

class TestScenario07_AuroraPromotion:
    def test_promotion_pending_to_confirmed(self, backend):
        """Aurora promotion pending transitions to confirmed state."""
        backend.put_state({
            "active_region": SECONDARY_REGION,
            "state": "WAITING_AURORA_PROMOTION",
            "latch_engaged": True,
            "consecutive_failures": 0,
            "aurora_promotion_pending": True,
            "last_failover_ts": datetime.now(timezone.utc).isoformat(),
        })

        # Operator promotes Aurora. Orchestrator detects it.
        backend.update_state({
            "aurora_promotion_pending": False,
            "state": "SECONDARY_ACTIVE",
        })

        state = backend.get_state()
        assert state["aurora_promotion_pending"] is False
        assert state["state"] == "SECONDARY_ACTIVE"
        assert state["latch_engaged"] is True  # latch stays until failback
        assert state["active_region"] == SECONDARY_REGION


# ===========================================================================
# Scenario 8: Failback
# ===========================================================================

class TestScenario08_Failback:
    def test_full_failback_lifecycle(self, backend):
        """Operator runs failback: latch released, state returns to PRIMARY_ACTIVE."""
        # Start in post-failover state (secondary active, latch engaged)
        backend.put_state({
            "active_region": SECONDARY_REGION,
            "state": "SECONDARY_ACTIVE",
            "latch_engaged": True,
            "consecutive_failures": 0,
            "aurora_promotion_pending": False,
            "last_failover_ts": (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat(),
        })

        # Failback Lambda: set FAILBACK_IN_PROGRESS
        backend.update_state({
            "state": "FAILBACK_IN_PROGRESS",
            "initiated_by": "MANUAL",
            "reason": "Manual failback to us-east-1",
        })

        state = backend.get_state()
        assert state["state"] == "FAILBACK_IN_PROGRESS"

        # Failback Lambda validates target health, then completes
        backend.update_state({
            "active_region": PRIMARY_REGION,
            "state": "PRIMARY_ACTIVE",
            "latch_engaged": False,  # RELEASED
            "consecutive_failures": 0,
            "last_failover_ts": datetime.now(timezone.utc).isoformat(),
        })

        state = backend.get_state()
        assert state["active_region"] == PRIMARY_REGION
        assert state["state"] == "PRIMARY_ACTIVE"
        assert state["latch_engaged"] is False
        assert state["consecutive_failures"] == 0

    def test_failback_clears_latch(self, backend):
        """Latch is only cleared by explicit failback update."""
        backend.put_state({
            "active_region": SECONDARY_REGION,
            "state": "SECONDARY_ACTIVE",
            "latch_engaged": True,
        })

        # Latch is True
        assert backend.get_state()["latch_engaged"] is True

        # Update something else — latch stays
        backend.update_state({"consecutive_failures": 0})
        assert backend.get_state()["latch_engaged"] is True

        # Explicit failback clears it
        backend.update_state({"latch_engaged": False})
        assert backend.get_state()["latch_engaged"] is False


# ===========================================================================
# Scenario 9: Cooldown enforcement
# ===========================================================================

class TestScenario09_CooldownEnforcement:
    def test_failover_blocked_during_cooldown(self, backend):
        """Second failover within cooldown window is rejected."""
        recent_failover = datetime.now(timezone.utc) - timedelta(minutes=10)

        backend.put_state({
            "active_region": SECONDARY_REGION,
            "state": "SECONDARY_ACTIVE",
            "latch_engaged": False,
            "consecutive_failures": CONSECUTIVE_FAILURES_THRESHOLD,
            "last_failover_ts": recent_failover.isoformat(),
            "cooldown_minutes": COOLDOWN_MINUTES,
        })

        # Active handler checks cooldown before claiming failover
        state = backend.get_state()
        last_ts = datetime.fromisoformat(
            state["last_failover_ts"].replace("Z", "+00:00")
        )
        cooldown_window = timedelta(minutes=state.get("cooldown_minutes", COOLDOWN_MINUTES))
        now = datetime.now(timezone.utc)

        in_cooldown = now < last_ts + cooldown_window
        assert in_cooldown is True, "Should be within cooldown window"

        # Orchestrator would NOT attempt failover — just warn and return

    def test_failover_allowed_after_cooldown(self, backend):
        """Failover proceeds after cooldown expires."""
        old_failover = datetime.now(timezone.utc) - timedelta(minutes=COOLDOWN_MINUTES + 5)

        backend.put_state({
            "active_region": PRIMARY_REGION,
            "state": "PRIMARY_ACTIVE",
            "latch_engaged": False,
            "consecutive_failures": CONSECUTIVE_FAILURES_THRESHOLD,
            "last_failover_ts": old_failover.isoformat(),
            "cooldown_minutes": COOLDOWN_MINUTES,
        })

        state = backend.get_state()
        last_ts = datetime.fromisoformat(
            state["last_failover_ts"].replace("Z", "+00:00")
        )
        cooldown_window = timedelta(minutes=state.get("cooldown_minutes", COOLDOWN_MINUTES))
        now = datetime.now(timezone.utc)

        in_cooldown = now < last_ts + cooldown_window
        assert in_cooldown is False, "Should be past cooldown"

        # Proceed with failover claim
        claimed = backend.conditional_update(
            "state", "PRIMARY_ACTIVE",
            {
                "state": "WAITING_AURORA_PROMOTION",
                "active_region": SECONDARY_REGION,
                "latch_engaged": True,
                "last_failover_ts": now.isoformat(),
            },
        )
        assert claimed is True


# ===========================================================================
# Scenario 10: State reset
# ===========================================================================

class TestScenario10_StateReset:
    def test_reset_clears_everything(self, backend):
        """Operator reset restores clean PRIMARY_ACTIVE state."""
        # Start in messy post-failover state
        backend.put_state({
            "active_region": SECONDARY_REGION,
            "state": "WAITING_AURORA_PROMOTION",
            "latch_engaged": True,
            "consecutive_failures": 5,
            "aurora_promotion_pending": True,
            "last_failover_ts": datetime.now(timezone.utc).isoformat(),
            "initiated_by": "AUTO_ACTIVE",
            "reason": "Something went wrong",
        })

        # Operator invokes reset
        now = datetime.now(timezone.utc)
        backend.put_state({
            "active_region": PRIMARY_REGION,
            "state": "PRIMARY_ACTIVE",
            "last_failover_ts": "1970-01-01T00:00:00Z",
            "cooldown_minutes": COOLDOWN_MINUTES,
            "initiated_by": "MANUAL_RESET",
            "reason": f"State reset at {now.isoformat()}",
            "latch_engaged": False,
            "consecutive_failures": 0,
            "last_active_metric_ts": now.isoformat(),
            "aurora_promotion_pending": False,
            "last_warning_notification_ts": "1970-01-01T00:00:00Z",
        })

        state = backend.get_state()
        assert state["active_region"] == PRIMARY_REGION
        assert state["state"] == "PRIMARY_ACTIVE"
        assert state["latch_engaged"] is False
        assert state["consecutive_failures"] == 0
        assert state["aurora_promotion_pending"] is False
        assert state["initiated_by"] == "MANUAL_RESET"


# ===========================================================================
# Scenario 11: Manual failover mode
# ===========================================================================

class TestScenario11_ManualFailover:
    def test_manual_execute_failover(self, backend):
        """Operator triggers manual failover via execute_failover event."""
        backend.put_state(_default_state())

        state = backend.get_state()
        active_region = state["active_region"]
        target_region = SECONDARY_REGION if active_region == PRIMARY_REGION else PRIMARY_REGION

        now = datetime.now(timezone.utc)
        expected_state = "PRIMARY_ACTIVE" if active_region == PRIMARY_REGION else "SECONDARY_ACTIVE"

        claimed = backend.conditional_update(
            "state", expected_state,
            {
                "state": "WAITING_AURORA_PROMOTION",
                "active_region": target_region,
                "last_failover_ts": now.isoformat(),
                "latch_engaged": True,
                "consecutive_failures": 0,
                "initiated_by": "MANUAL_EXECUTE",
                "reason": "Manual failover requested by operator",
                "aurora_promotion_pending": True,
            },
        )
        assert claimed is True

        state = backend.get_state()
        assert state["active_region"] == target_region
        assert state["initiated_by"] == "MANUAL_EXECUTE"
        assert state["latch_engaged"] is True


# ===========================================================================
# Scenario 12: Region-level failure (passive detects stale active)
# ===========================================================================

class TestScenario12_RegionLevelFailure:
    def test_passive_detects_stale_and_claims(self, backend):
        """Passive region detects stale heartbeat and claims failover."""
        stale_ts = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()

        backend.put_state({
            "active_region": PRIMARY_REGION,
            "state": "PRIMARY_ACTIVE",
            "latch_engaged": False,
            "consecutive_failures": 0,
            "last_active_metric_ts": stale_ts,  # 5 min old = stale
            "last_failover_ts": "1970-01-01T00:00:00Z",
            "aurora_promotion_pending": False,
        })

        # Passive region (us-east-2) reads state
        state = backend.get_state()
        last_active = datetime.fromisoformat(
            state["last_active_metric_ts"].replace("Z", "+00:00")
        )
        stale_threshold = timedelta(minutes=3)
        age = datetime.now(timezone.utc) - last_active

        assert age > stale_threshold, "Active region should be stale"

        # Passive claims failover
        now = datetime.now(timezone.utc)
        claimed = backend.conditional_update(
            "state", "PRIMARY_ACTIVE",
            {
                "state": "WAITING_AURORA_PROMOTION",
                "active_region": SECONDARY_REGION,
                "last_failover_ts": now.isoformat(),
                "latch_engaged": True,
                "consecutive_failures": 0,
                "initiated_by": "AUTO_PASSIVE",
                "reason": "Region-level failure: heartbeat stale >3min",
                "aurora_promotion_pending": True,
            },
        )
        assert claimed is True

        state = backend.get_state()
        assert state["active_region"] == SECONDARY_REGION
        assert state["initiated_by"] == "AUTO_PASSIVE"
        assert state["latch_engaged"] is True

    def test_passive_claim_blocked_if_active_already_handled(self, backend):
        """If active region already changed state, passive claim fails."""
        backend.put_state({
            "active_region": PRIMARY_REGION,
            "state": "PRIMARY_ACTIVE",
            "latch_engaged": False,
        })

        # Active region transitions to WAITING_AURORA_PROMOTION first
        backend.conditional_update(
            "state", "PRIMARY_ACTIVE",
            {"state": "WAITING_AURORA_PROMOTION", "active_region": SECONDARY_REGION},
        )

        # Passive region tries to claim (stale read of PRIMARY_ACTIVE)
        claimed = backend.conditional_update(
            "state", "PRIMARY_ACTIVE",
            {"state": "WAITING_AURORA_PROMOTION", "active_region": SECONDARY_REGION},
        )
        assert claimed is False  # lost the race


# ===========================================================================
# Scenario 13: Full lifecycle — init → degrade → failover → aurora → failback
# ===========================================================================

class TestScenario13_FullLifecycle:
    def test_complete_failover_and_failback(self, backend):
        """
        Complete lifecycle:
          1. Initialize PRIMARY_ACTIVE
          2. Health degrades — failures accumulate
          3. Threshold reached — failover to secondary
          4. Latch blocks old region
          5. Aurora promotion confirmed
          6. Operator runs failback
          7. State returns to PRIMARY_ACTIVE
        """
        print("\n  Step 1: Initialize")
        backend.put_state(_default_state())
        state = backend.get_state()
        assert state["state"] == "PRIMARY_ACTIVE"
        assert state["active_region"] == PRIMARY_REGION

        print("  Step 2: Health degrades — 3 consecutive failures")
        for i in range(CONSECUTIVE_FAILURES_THRESHOLD):
            ok = backend.conditional_update(
                "consecutive_failures", i, {"consecutive_failures": i + 1}
            )
            assert ok is True
        state = backend.get_state()
        assert state["consecutive_failures"] == 3

        print("  Step 3: Threshold reached — failover to us-east-2")
        now = datetime.now(timezone.utc)
        claimed = backend.conditional_update(
            "state", "PRIMARY_ACTIVE",
            {
                "state": "WAITING_AURORA_PROMOTION",
                "active_region": SECONDARY_REGION,
                "last_failover_ts": now.isoformat(),
                "latch_engaged": True,
                "consecutive_failures": 0,
                "initiated_by": "AUTO_ACTIVE",
                "reason": "Health check threshold exceeded",
                "aurora_promotion_pending": True,
            },
        )
        assert claimed is True
        state = backend.get_state()
        assert state["state"] == "WAITING_AURORA_PROMOTION"
        assert state["active_region"] == SECONDARY_REGION
        assert state["latch_engaged"] is True

        print("  Step 4: Latch blocks old region (us-east-1)")
        # Simulate passive handler in us-east-1: reads state, sees latch
        state = backend.get_state()
        assert state["latch_engaged"] is True
        # Passive handler would publish metric=0 and return

        print("  Step 5: Aurora promotion confirmed")
        backend.update_state({
            "aurora_promotion_pending": False,
            "state": "SECONDARY_ACTIVE",
        })
        state = backend.get_state()
        assert state["state"] == "SECONDARY_ACTIVE"
        assert state["aurora_promotion_pending"] is False
        assert state["latch_engaged"] is True  # stays until failback

        print("  Step 6: Operator runs failback to us-east-1")
        backend.update_state({
            "state": "FAILBACK_IN_PROGRESS",
            "initiated_by": "MANUAL",
            "reason": "Failback to primary after incident resolved",
        })
        state = backend.get_state()
        assert state["state"] == "FAILBACK_IN_PROGRESS"

        # Failback completes
        backend.update_state({
            "active_region": PRIMARY_REGION,
            "state": "PRIMARY_ACTIVE",
            "latch_engaged": False,
            "consecutive_failures": 0,
            "last_failover_ts": datetime.now(timezone.utc).isoformat(),
        })

        print("  Step 7: Verify state is back to PRIMARY_ACTIVE")
        state = backend.get_state()
        assert state["active_region"] == PRIMARY_REGION
        assert state["state"] == "PRIMARY_ACTIVE"
        assert state["latch_engaged"] is False
        assert state["consecutive_failures"] == 0
        print("  Full lifecycle complete!")


# ===========================================================================
# Scenario 14: Cross-region — write in primary, read from secondary
# ===========================================================================

CRR_TEST = os.environ.get("CRR_TEST", "0") == "1"


class TestScenario14_CrossRegionConsistency:
    """Test that state written in one region is readable from the other via CRR."""

    @pytest.fixture(autouse=True)
    def skip_unless_crr(self):
        if not CRR_TEST:
            pytest.skip("Set CRR_TEST=1 with CRR_PRIMARY_BUCKET and CRR_SECONDARY_BUCKET")

    def test_failover_state_replicates(self):
        primary_bucket = os.environ["CRR_PRIMARY_BUCKET"]
        secondary_bucket = os.environ["CRR_SECONDARY_BUCKET"]
        # Must use failover-state/ prefix to match the CRR replication rule
        test_prefix = "failover-state/"

        primary = S3StateBackend(bucket=primary_bucket, region=PRIMARY_REGION, prefix=test_prefix)
        secondary = S3StateBackend(bucket=secondary_bucket, region=SECONDARY_REGION, prefix=test_prefix)

        # Write failover state to primary
        now = datetime.now(timezone.utc)
        primary.put_state({
            "active_region": SECONDARY_REGION,
            "state": "WAITING_AURORA_PROMOTION",
            "latch_engaged": True,
            "consecutive_failures": 0,
            "last_failover_ts": now.isoformat(),
            "initiated_by": "AUTO_ACTIVE",
            "reason": "E2E CRR test",
            "aurora_promotion_pending": True,
        })

        # Poll secondary
        replicated = False
        for i in range(30):  # up to 150s
            time.sleep(5)
            state = secondary.get_state()
            if state.get("state") == "WAITING_AURORA_PROMOTION":
                replicated = True
                print(f"  CRR replication took ~{(i+1)*5}s")
                # Verify full state integrity
                assert state["active_region"] == SECONDARY_REGION
                assert state["latch_engaged"] is True
                assert state["initiated_by"] == "AUTO_ACTIVE"
                assert state["aurora_promotion_pending"] is True
                break

        assert replicated, "Failover state did not replicate within 150s"

        # Cleanup
        s3 = boto3.client("s3", region_name=PRIMARY_REGION)
        s3.delete_object(
            Bucket=primary_bucket,
            Key="failover-state/REGION_STATE.json",
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
