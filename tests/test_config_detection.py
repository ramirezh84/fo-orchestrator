#!/usr/bin/env python3
"""Tests for v1.6 PR1: ENVIRONMENT env var + detect_data_tier_config() helper.

Covers:
  - _format_subject() composition rules across all ENVIRONMENT × APP_NAME states.
  - detect_data_tier_config() for all 9 baseline configurations (C1–C9):
      Aurora ∈ {absent, manual, auto} × Redis ∈ {absent, manual, auto}
  - Parity between orchestrator and manual_failback_v2 helpers (must match).

PR1 introduces no behavior change; these tests pin the new helpers' contracts
so subsequent PRs (notification template, failback gates, retry caps) can
build on them.

Note on style: matches tests/test_orchestrator.py — env vars are set once
before module import via os.environ; per-test variation is done via
patch.object() to avoid sys.modules pollution that breaks downstream tests.

Run: python3 -m pytest tests/test_config_detection.py -v
"""

import os
from unittest.mock import MagicMock, patch

import pytest


# Minimum env required for both modules to import without crashing.
_MIN_ENV = {
    "SNS_TOPIC_ARN": "arn:aws:sns:us-east-1:123456789012:failover-alerts",
    "AWS_REGION": "us-east-1",
    "PRIMARY_REGION": "us-east-1",
    "SECONDARY_REGION": "us-east-2",
    "STATE_BACKEND": "dynamodb",
    "STATE_TABLE": "failover-state",
}

for k, v in _MIN_ENV.items():
    os.environ.setdefault(k, v)

# Mock boto3 and the state backend at module level so the orchestrator's
# top-level client creation does not make real AWS calls. Same pattern as
# tests/test_orchestrator.py so the two test files share a consistent
# mocked module instance via the import cache.
_mock_boto3_patcher = patch("boto3.client")
_mock_boto3_client = _mock_boto3_patcher.start()
_mock_boto3_client.return_value = MagicMock()

_mock_create_backend_patcher = patch("state_backend.create_backend")
_mock_create_backend = _mock_create_backend_patcher.start()
_mock_create_backend.return_value = MagicMock()

import failover_orchestrator_v3 as orch  # noqa: E402
import manual_failback_v2 as failback    # noqa: E402

_mock_boto3_patcher.stop()
_mock_create_backend_patcher.stop()


# ---------------------------------------------------------------------------
# _format_subject — composition rules
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("environment,app_name,subject,expected", [
    # Both set → [ENVIRONMENT-APP_NAME] prefix (the new shape).
    ("prod", "critical-app", "FAILOVER", "[prod-critical-app] FAILOVER"),
    ("staging", "billing", "WARNING", "[staging-billing] WARNING"),
    ("demo", "fo-v10x-s1", "Test", "[demo-fo-v10x-s1] Test"),

    # APP_NAME only → [APP_NAME] (backwards-compat with v1.0–v1.5).
    ("", "critical-app", "FAILOVER", "[critical-app] FAILOVER"),

    # ENVIRONMENT only → [ENVIRONMENT].
    ("prod", "", "FAILOVER", "[prod] FAILOVER"),

    # Neither set → bare subject (no prefix at all).
    ("", "", "FAILOVER", "FAILOVER"),
])
def test_format_subject_orchestrator(environment, app_name, subject, expected):
    """Orchestrator's _format_subject composes prefix per environment/app_name."""
    with patch.object(orch, "ENVIRONMENT", environment), \
         patch.object(orch, "APP_NAME", app_name):
        assert orch._format_subject(subject) == expected


@pytest.mark.parametrize("environment,app_name,subject,expected", [
    ("prod", "critical-app", "FAILBACK COMPLETE", "[prod-critical-app] FAILBACK COMPLETE"),
    ("", "critical-app", "FAILBACK COMPLETE", "[critical-app] FAILBACK COMPLETE"),
    ("prod", "", "FAILBACK COMPLETE", "[prod] FAILBACK COMPLETE"),
    ("", "", "FAILBACK COMPLETE", "FAILBACK COMPLETE"),
])
def test_format_subject_failback(environment, app_name, subject, expected):
    """Failback Lambda's _format_subject must match orchestrator behavior."""
    with patch.object(failback, "ENVIRONMENT", environment), \
         patch.object(failback, "APP_NAME", app_name):
        assert failback._format_subject(subject) == expected


def test_format_subject_truncates_to_100_chars():
    """SNS subject limit is 100 chars; long inputs are truncated."""
    with patch.object(orch, "ENVIRONMENT", "prod"), \
         patch.object(orch, "APP_NAME", "critical-app"):
        long_subject = "X" * 200
        result = orch._format_subject(long_subject)
        assert len(result) == 100
        assert result.startswith("[prod-critical-app] ")


# ---------------------------------------------------------------------------
# detect_data_tier_config — all 9 baseline configurations (C1–C9)
# ---------------------------------------------------------------------------

# Each row: (id, aurora_cluster_id, aurora_auto, redis_global_rg, redis_auto, expected)
_CONFIG_MATRIX = [
    ("C1", "",       False, "",          False,
     {"aurora_present": False, "aurora_auto": False, "redis_present": False, "redis_auto": False}),
    ("C2", "ac-w1",  False, "",          False,
     {"aurora_present": True,  "aurora_auto": False, "redis_present": False, "redis_auto": False}),
    ("C3", "ac-w1",  True,  "",          False,
     {"aurora_present": True,  "aurora_auto": True,  "redis_present": False, "redis_auto": False}),
    ("C4", "",       False, "rg-global", False,
     {"aurora_present": False, "aurora_auto": False, "redis_present": True,  "redis_auto": False}),
    ("C5", "ac-w1",  False, "rg-global", False,
     {"aurora_present": True,  "aurora_auto": False, "redis_present": True,  "redis_auto": False}),
    ("C6", "ac-w1",  True,  "rg-global", False,
     {"aurora_present": True,  "aurora_auto": True,  "redis_present": True,  "redis_auto": False}),
    ("C7", "",       False, "rg-global", True,
     {"aurora_present": False, "aurora_auto": False, "redis_present": True,  "redis_auto": True}),
    ("C8", "ac-w1",  False, "rg-global", True,
     {"aurora_present": True,  "aurora_auto": False, "redis_present": True,  "redis_auto": True}),
    ("C9", "ac-w1",  True,  "rg-global", True,
     {"aurora_present": True,  "aurora_auto": True,  "redis_present": True,  "redis_auto": True}),
]


def _env_for(aurora_id, aurora_auto, redis_id, redis_auto):
    """Build the os.environ patch dict for a configuration row."""
    return {
        "AURORA_CLUSTER_ID": aurora_id,
        "AURORA_AUTO_PROMOTE": "true" if aurora_auto else "false",
        "ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID": redis_id,
        "ELASTICACHE_AUTO_PROMOTE": "true" if redis_auto else "false",
    }


@pytest.mark.parametrize("cid,aurora_id,aurora_auto,redis_id,redis_auto,expected",
                         _CONFIG_MATRIX, ids=[c[0] for c in _CONFIG_MATRIX])
def test_detect_data_tier_config_orchestrator(cid, aurora_id, aurora_auto, redis_id, redis_auto, expected):
    """Orchestrator's detect_data_tier_config returns correct flags for C1–C9.

    PR3c: helper now reads os.environ at call time so portal env-var flips
    take effect without a Lambda cold start (and so per-test patches don't
    require module attribute manipulation).
    """
    with patch.dict(os.environ, _env_for(aurora_id, aurora_auto, redis_id, redis_auto)):
        assert orch.detect_data_tier_config() == expected


@pytest.mark.parametrize("cid,aurora_id,aurora_auto,redis_id,redis_auto,expected",
                         _CONFIG_MATRIX, ids=[c[0] for c in _CONFIG_MATRIX])
def test_detect_data_tier_config_failback(cid, aurora_id, aurora_auto, redis_id, redis_auto, expected):
    """Failback Lambda's helper must match the orchestrator exactly."""
    with patch.dict(os.environ, _env_for(aurora_id, aurora_auto, redis_id, redis_auto)):
        assert failback.detect_data_tier_config() == expected


def test_aurora_auto_requires_aurora_present():
    """AURORA_AUTO_PROMOTE=true with empty AURORA_CLUSTER_ID → aurora_auto=False (defensive)."""
    with patch.dict(os.environ, {"AURORA_CLUSTER_ID": "",
                                  "AURORA_AUTO_PROMOTE": "true"}):
        cfg = orch.detect_data_tier_config()
        assert cfg["aurora_present"] is False
        assert cfg["aurora_auto"] is False  # cannot auto-promote a tier that isn't there


def test_redis_auto_requires_redis_present():
    """ELASTICACHE_AUTO_PROMOTE=true with empty global RG ID → redis_auto=False."""
    with patch.dict(os.environ, {"ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID": "",
                                  "ELASTICACHE_AUTO_PROMOTE": "true"}):
        cfg = orch.detect_data_tier_config()
        assert cfg["redis_present"] is False
        assert cfg["redis_auto"] is False


# ---------------------------------------------------------------------------
# F4 (PR4): failback Lambda's HEALTH_ENDPOINT default
# ---------------------------------------------------------------------------

def test_failback_health_endpoint_default_is_shallow():
    """v1.6 F4: failback Lambda defaults HEALTH_ENDPOINT to /healthcheck (shallow).

    The previous default `/actuator/health` is Spring Boot-specific and typically
    includes DB connectivity checks. CLAUDE.md recommends `/healthcheck` (app-only)
    to avoid the false-positives from DB-config issues that blocked the v1.5 drill's
    failback step. Operators who genuinely want deep validation can override via env.
    """
    # Module-level constant reflects the default at import time (no env var set).
    # Since other test files may have set HEALTH_ENDPOINT via setdefault, we
    # verify the source-of-truth: the default literal in the os.environ.get call.
    import inspect
    src = inspect.getsource(failback)
    # Look for the default in the literal getter (one line, deterministic).
    assert 'os.environ.get("HEALTH_ENDPOINT", "/healthcheck")' in src, (
        "Failback Lambda's HEALTH_ENDPOINT default must be /healthcheck per F4. "
        "If you intentionally changed it, update this test."
    )
