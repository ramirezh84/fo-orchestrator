#!/usr/bin/env python3
"""Tests for v1.6 PR2: notifications.compose_message() + throttle reform.

Pins the contract that PR3 will use to rewrite all 29 SNS call sites:

  - Required fields (severity, what, why, next_step) are validated.
  - Severity must be one of INFO/WARNING/CRITICAL.
  - Body always contains WHAT IS HAPPENING / WHAT TO DO NEXT sections.
  - Optional sections (CONTEXT table, journey breadcrumb) render only when given.
  - Footer carries version / timestamp / source / region in a stable format.
  - Throttle reform: default cooldown is 5min (was 10min in v1.5).
  - Throttle bypass via bypass_throttle=True for first-failure / retry / escalation
    notifications that must never be silenced by recent traffic.

Run: python3 -m pytest tests/test_notification_template.py -v
"""

import os
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest


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

# Mock boto3 + state backend before importing the orchestrator (matches the
# pattern in test_orchestrator.py / test_config_detection.py).
_mock_boto3_patcher = patch("boto3.client")
_mock_boto3_patcher.start().return_value = MagicMock()
_mock_create_backend_patcher = patch("state_backend.create_backend")
_mock_create_backend_patcher.start().return_value = MagicMock()

import notifications  # noqa: E402
import failover_orchestrator_v3 as orch  # noqa: E402

_mock_boto3_patcher.stop()
_mock_create_backend_patcher.stop()


# ---------------------------------------------------------------------------
# compose_message: required-field validation
# ---------------------------------------------------------------------------

def test_compose_rejects_invalid_severity():
    with pytest.raises(ValueError, match="severity"):
        notifications.compose_message(
            severity="WHATEVER", what="x", why="y", next_step="z",
        )


@pytest.mark.parametrize("missing", ["what", "why", "next_step"])
def test_compose_rejects_missing_required_field(missing):
    """Empty required field is a programmer error — fail loudly, not silently."""
    args = {
        "severity": notifications.SEVERITY_WARNING,
        "what": "ok",
        "why": "ok",
        "next_step": "ok",
    }
    args[missing] = ""
    with pytest.raises(ValueError, match=missing):
        notifications.compose_message(**args)


@pytest.mark.parametrize("severity", [
    notifications.SEVERITY_INFO,
    notifications.SEVERITY_WARNING,
    notifications.SEVERITY_CRITICAL,
])
def test_compose_accepts_all_three_severities(severity):
    subject, _ = notifications.compose_message(
        severity=severity, what="x", why="y", next_step="z",
    )
    assert subject.startswith(severity + ": ")


# ---------------------------------------------------------------------------
# compose_message: subject is bare "<SEVERITY>: <what>" (no env-app prefix)
# ---------------------------------------------------------------------------

def test_subject_does_not_include_env_app_prefix():
    """Caller's _format_subject() handles the [ENVIRONMENT-APP_NAME] prefix."""
    subject, _ = notifications.compose_message(
        severity="WARNING",
        what="Region us-west-1 reported its FIRST health failure (1 of 3)",
        why="HTTP /healthcheck returned 503 from one of three configured signals.",
        next_step="No action — system will retry.",
    )
    assert subject == "WARNING: Region us-west-1 reported its FIRST health failure (1 of 3)"
    # No bracketed prefix here — that gets added by the Lambda's _format_subject.
    assert not subject.startswith("[")


# ---------------------------------------------------------------------------
# compose_message: body sections in order
# ---------------------------------------------------------------------------

def _fixed_now():
    return datetime(2026, 4, 25, 20, 30, 0, tzinfo=timezone.utc)


def test_body_minimum_content_includes_what_why_next():
    _, body = notifications.compose_message(
        severity="WARNING",
        what="Region us-west-1 unhealthy",
        why="HTTP 503 from /healthcheck.",
        next_step="No action.",
        now=_fixed_now(),
    )
    # All three required headings present, in order.
    what_idx = body.index("WHAT IS HAPPENING")
    next_idx = body.index("WHAT TO DO NEXT")
    assert what_idx < next_idx
    assert "HTTP 503 from /healthcheck." in body
    assert "No action." in body


def test_body_renders_journey_when_provided():
    _, body = notifications.compose_message(
        severity="WARNING", what="x", why="y", next_step="z",
        journey=[
            "[1] First failure",
            "[ ] Sustained",
            "[ ] Failover",
        ],
        now=_fixed_now(),
    )
    assert "WHERE WE ARE IN THE INCIDENT" in body
    assert "[1] First failure" in body
    assert "[ ] Failover" in body


def test_body_omits_journey_section_when_not_provided():
    _, body = notifications.compose_message(
        severity="WARNING", what="x", why="y", next_step="z",
        now=_fixed_now(),
    )
    assert "WHERE WE ARE IN THE INCIDENT" not in body


def test_body_renders_context_as_aligned_table():
    _, body = notifications.compose_message(
        severity="WARNING", what="x", why="y", next_step="z",
        context={
            "Active region": "us-west-1",
            "Consecutive failures": "1 of 3",
            "Last heartbeat": "2026-04-25T20:29:30Z",
        },
        now=_fixed_now(),
    )
    assert "CONTEXT" in body
    # Labels left-padded to a common width — spot-check alignment.
    assert "Active region" in body
    assert "Consecutive failures" in body
    # Each line uses " : " separator.
    assert " : us-west-1" in body
    assert " : 1 of 3" in body


def test_body_omits_context_section_when_not_provided():
    _, body = notifications.compose_message(
        severity="WARNING", what="x", why="y", next_step="z",
        now=_fixed_now(),
    )
    assert "CONTEXT" not in body


# ---------------------------------------------------------------------------
# compose_message: footer
# ---------------------------------------------------------------------------

def test_footer_contains_version_and_timestamp():
    _, body = notifications.compose_message(
        severity="WARNING", what="x", why="y", next_step="z",
        version="v1.6", now=_fixed_now(),
    )
    assert "Vigil v1.6" in body
    assert "2026-04-25T20:30:00+00:00" in body


def test_footer_includes_source_and_region_when_provided():
    _, body = notifications.compose_message(
        severity="WARNING", what="x", why="y", next_step="z",
        source="failover-orchestrator", region="us-west-1",
        now=_fixed_now(),
    )
    assert "failover-orchestrator" in body
    assert "region=us-west-1" in body


def test_footer_omits_source_and_region_when_empty():
    _, body = notifications.compose_message(
        severity="WARNING", what="x", why="y", next_step="z",
        source="", region="",
        now=_fixed_now(),
    )
    # Footer should still have version + timestamp, but neither source nor region tokens.
    assert "Vigil" in body
    assert "region=" not in body


# ---------------------------------------------------------------------------
# Throttle reform on send_warning_notification
# ---------------------------------------------------------------------------

class TestWarningThrottleReform:
    """v1.6 lowers the WARNING throttle to 5min and adds bypass_throttle=True."""

    def test_default_cooldown_is_five_minutes(self):
        """Default dropped from 10min → 5min in v1.6."""
        assert orch.WARNING_NOTIFICATION_COOLDOWN_MINUTES == 5

    @patch.object(orch, "sns")
    @patch.object(orch, "update_failover_state")
    def test_throttled_when_recent_warning_and_no_bypass(self, mock_update, mock_sns):
        """Existing behavior: a recent warning suppresses the next one."""
        recent = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
        state = {"last_warning_notification_ts": recent}

        orch.send_warning_notification("WARNING: x", "body", state)

        mock_sns.publish.assert_not_called()
        mock_update.assert_not_called()

    @patch.object(orch, "sns")
    @patch.object(orch, "update_failover_state")
    def test_bypass_throttle_sends_even_when_recent(self, mock_update, mock_sns):
        """First-failure / retry-N / escalation notifications bypass the cooldown."""
        recent = (datetime.now(timezone.utc) - timedelta(seconds=30)).isoformat()
        state = {"last_warning_notification_ts": recent}

        orch.send_warning_notification(
            "WARNING: First failure", "body", state, bypass_throttle=True,
        )

        # SNS was called despite recent warning timestamp.
        mock_sns.publish.assert_called_once()
        # State update still happens (so subsequent throttled calls see fresh ts).
        mock_update.assert_called_once()

    @patch.object(orch, "sns")
    @patch.object(orch, "update_failover_state")
    def test_no_throttle_when_no_prior_warning(self, mock_update, mock_sns):
        """The very first WARNING ever (epoch ts) always sends."""
        state = {"last_warning_notification_ts": "1970-01-01T00:00:00Z"}

        orch.send_warning_notification("WARNING: x", "body", state)

        mock_sns.publish.assert_called_once()
        mock_update.assert_called_once()

    @patch.object(orch, "sns")
    @patch.object(orch, "update_failover_state")
    def test_no_throttle_after_cooldown_window(self, mock_update, mock_sns):
        """After WARNING_NOTIFICATION_COOLDOWN_MINUTES elapse, next warning sends."""
        long_ago = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
        state = {"last_warning_notification_ts": long_ago}

        orch.send_warning_notification("WARNING: x", "body", state)

        mock_sns.publish.assert_called_once()
        mock_update.assert_called_once()
