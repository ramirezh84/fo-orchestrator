#!/usr/bin/env python3
"""
Tests for the AI Aurora promotion advisor module.

Run: python3 -m pytest tests/test_aurora_advisor.py -v
"""

import json
import os
from unittest.mock import patch

import pytest

os.environ.setdefault("SNS_TOPIC_ARN", "arn:aws:sns:us-east-1:123456789:test")
os.environ.setdefault("AI_RCA_ENABLED", "false")


def _make_stability_context(**overrides):
    """Build a sample stability context with optional overrides."""
    ctx = {
        "region": "us-west-2",
        "window_minutes": 10,
        "aurora_replication_lag": {
            "replica_count": 1,
            "replicas": {
                "reader-1": {
                    "datapoints": [
                        {"timestamp": "t1", "value": 3.0},
                        {"timestamp": "t2", "value": 4.0},
                        {"timestamp": "t3", "value": 3.5},
                        {"timestamp": "t4", "value": 3.0},
                        {"timestamp": "t5", "value": 3.2},
                    ],
                    "summary": {"min": 3.0, "max": 4.0, "avg": 3.34, "latest": 3.2, "datapoint_count": 5},
                }
            },
        },
        "aurora_cluster_detail": {"status": "available", "is_writer": False},
        "aurora_instance_status": {
            "instances": [
                {"instance_id": "writer-1", "is_writer": True, "status": "available"},
                {"instance_id": "reader-1", "is_writer": False, "status": "available"},
            ]
        },
        "aurora_global_topology": {
            "status": "available",
            "members": [
                {"cluster_arn": "arn:primary", "is_writer": True, "synchronization_status": "connected"},
                {"cluster_arn": "arn:secondary", "is_writer": False, "synchronization_status": "connected"},
            ],
        },
        "aurora_events": {"events": []},
    }
    ctx.update(overrides)
    return ctx


# ===========================================================================
# Parsing Tests
# ===========================================================================


class TestParseAdvisorResponse:
    """Tests for LLM response parsing."""

    def test_parse_switchover_recommendation(self):
        from ai.aurora_advisor import _parse_advisor_response

        llm_text = '{"recommended_method": "switchover", "confidence": 95, "data_loss_risk": "none", "estimated_data_loss_ms": 0, "warnings": []}\n\nREASONING:\nLag is stable at 3ms.'

        result = _parse_advisor_response(llm_text, "app_failure")
        assert result["recommended_method"] == "switchover"
        assert result["confidence"] == 95
        assert result["data_loss_risk"] == "none"
        assert "stable" in result["reasoning"].lower()

    def test_parse_failover_recommendation(self):
        from ai.aurora_advisor import _parse_advisor_response

        llm_text = '{"recommended_method": "failover", "confidence": 75, "data_loss_risk": "medium", "estimated_data_loss_ms": 500, "warnings": ["Lag was high"]}'

        result = _parse_advisor_response(llm_text, "region_failure")
        assert result["recommended_method"] == "failover"
        assert result["estimated_data_loss_ms"] == 500

    def test_fallback_on_invalid_json(self):
        from ai.aurora_advisor import _parse_advisor_response

        result = _parse_advisor_response("Not JSON", "app_failure")
        assert result["recommended_method"] == "switchover"
        assert result["confidence"] == 50

    def test_region_failure_defaults_to_failover(self):
        from ai.aurora_advisor import _parse_advisor_response

        result = _parse_advisor_response("Not JSON", "region_failure")
        assert result["recommended_method"] == "failover"


# ===========================================================================
# Hard Guardrails Tests (Phase 3)
# ===========================================================================


class TestHardGuardrails:
    """Tests for deterministic safety checks."""

    def test_passes_when_all_healthy(self):
        from ai.aurora_advisor import _apply_hard_guardrails

        ctx = _make_stability_context()
        result = _apply_hard_guardrails(ctx, "app_failure")
        assert result["passed"] is True
        assert result["reasons"] == []

    def test_blocks_on_high_replication_lag(self):
        from ai.aurora_advisor import _apply_hard_guardrails

        ctx = _make_stability_context()
        ctx["aurora_replication_lag"]["replicas"]["reader-1"]["summary"]["max"] = 200.0

        result = _apply_hard_guardrails(ctx, "app_failure")
        assert result["passed"] is False
        assert any("exceeded" in r for r in result["reasons"])

    def test_blocks_on_consistently_high_lag(self):
        from ai.aurora_advisor import _apply_hard_guardrails

        ctx = _make_stability_context()
        ctx["aurora_replication_lag"]["replicas"]["reader-1"]["datapoints"] = [
            {"timestamp": "t1", "value": 150.0},
            {"timestamp": "t2", "value": 160.0},
            {"timestamp": "t3", "value": 155.0},
        ]
        ctx["aurora_replication_lag"]["replicas"]["reader-1"]["summary"]["max"] = 160.0

        result = _apply_hard_guardrails(ctx, "app_failure")
        assert result["passed"] is False

    def test_blocks_on_bad_sync_status(self):
        from ai.aurora_advisor import _apply_hard_guardrails

        ctx = _make_stability_context()
        ctx["aurora_global_topology"]["members"][1]["synchronization_status"] = "pending-resync"

        result = _apply_hard_guardrails(ctx, "app_failure")
        assert result["passed"] is False
        assert any("pending-resync" in r for r in result["reasons"])

    def test_blocks_on_bad_cluster_status(self):
        from ai.aurora_advisor import _apply_hard_guardrails

        ctx = _make_stability_context()
        ctx["aurora_cluster_detail"]["status"] = "modifying"

        result = _apply_hard_guardrails(ctx, "app_failure")
        assert result["passed"] is False
        assert any("modifying" in r for r in result["reasons"])

    def test_blocks_on_instance_in_transitional_state(self):
        from ai.aurora_advisor import _apply_hard_guardrails

        ctx = _make_stability_context()
        ctx["aurora_instance_status"]["instances"][0]["status"] = "rebooting"

        result = _apply_hard_guardrails(ctx, "app_failure")
        assert result["passed"] is False
        assert any("rebooting" in r for r in result["reasons"])

    def test_passes_with_available_and_backing_up(self):
        from ai.aurora_advisor import _apply_hard_guardrails

        ctx = _make_stability_context()
        ctx["aurora_cluster_detail"]["status"] = "backing-up"

        result = _apply_hard_guardrails(ctx, "app_failure")
        assert result["passed"] is True

    def test_handles_missing_data_gracefully(self):
        from ai.aurora_advisor import _apply_hard_guardrails

        ctx = {"region": "us-east-1", "window_minutes": 10}
        result = _apply_hard_guardrails(ctx, "app_failure")
        assert result["passed"] is True


# ===========================================================================
# Auto-Execute Decision Tests
# ===========================================================================


class TestShouldAutoExecute:
    """Tests for mode-based auto-execution logic."""

    def test_advisory_never_auto_executes(self):
        from ai.aurora_advisor import _should_auto_execute

        rec = {"confidence": 99, "recommended_method": "switchover"}
        assert _should_auto_execute(rec, "advisory") is False

    def test_guided_auto_executes_high_confidence_switchover(self):
        from ai.aurora_advisor import _should_auto_execute

        rec = {"confidence": 95, "recommended_method": "switchover"}
        assert _should_auto_execute(rec, "guided") is True

    def test_guided_blocks_low_confidence(self):
        from ai.aurora_advisor import _should_auto_execute

        rec = {"confidence": 70, "recommended_method": "switchover"}
        assert _should_auto_execute(rec, "guided") is False

    def test_guided_blocks_failover_method(self):
        from ai.aurora_advisor import _should_auto_execute

        rec = {"confidence": 99, "recommended_method": "failover"}
        assert _should_auto_execute(rec, "guided") is False

    def test_autonomous_always_auto_executes(self):
        from ai.aurora_advisor import _should_auto_execute

        rec = {"confidence": 60, "recommended_method": "failover"}
        assert _should_auto_execute(rec, "autonomous") is True

    def test_disabled_never_auto_executes(self):
        from ai.aurora_advisor import _should_auto_execute

        rec = {"confidence": 99, "recommended_method": "switchover"}
        assert _should_auto_execute(rec, "disabled") is False


# ===========================================================================
# Main Advisor Function Tests
# ===========================================================================


class TestAdviseAuroraPromotion:
    """Tests for the main advisor entry point."""

    def test_disabled_returns_empty(self):
        from ai.aurora_advisor import advise_aurora_promotion

        result = advise_aurora_promotion(
            _make_stability_context(), "app_failure", mode="disabled"
        )
        assert result["should_auto_execute"] is False
        assert result["recommended_method"] == ""

    @patch("ai.aurora_advisor.call_llm")
    def test_advisory_mode_returns_recommendation(self, mock_llm):
        from ai.aurora_advisor import advise_aurora_promotion

        mock_llm.return_value = '{"recommended_method": "switchover", "confidence": 92, "data_loss_risk": "none", "estimated_data_loss_ms": 0, "warnings": []}'

        result = advise_aurora_promotion(
            _make_stability_context(), "app_failure", mode="advisory"
        )

        assert result["recommended_method"] == "switchover"
        assert result["confidence"] == 92
        assert result["should_auto_execute"] is False
        assert result["guardrails_passed"] is True

    @patch("ai.aurora_advisor.call_llm")
    def test_guided_mode_auto_executes_on_high_confidence_switchover(self, mock_llm):
        from ai.aurora_advisor import advise_aurora_promotion

        mock_llm.return_value = '{"recommended_method": "switchover", "confidence": 95, "data_loss_risk": "none", "estimated_data_loss_ms": 0, "warnings": []}'

        result = advise_aurora_promotion(
            _make_stability_context(), "app_failure", mode="guided"
        )

        assert result["should_auto_execute"] is True
        assert result["recommended_method"] == "switchover"

    @patch("ai.aurora_advisor.call_llm")
    def test_guided_mode_blocks_failover_method(self, mock_llm):
        from ai.aurora_advisor import advise_aurora_promotion

        mock_llm.return_value = '{"recommended_method": "failover", "confidence": 95, "data_loss_risk": "medium", "estimated_data_loss_ms": 200, "warnings": []}'

        result = advise_aurora_promotion(
            _make_stability_context(), "app_failure", mode="guided"
        )

        assert result["should_auto_execute"] is False

    @patch("ai.aurora_advisor.call_llm")
    def test_autonomous_mode_auto_executes(self, mock_llm):
        from ai.aurora_advisor import advise_aurora_promotion

        mock_llm.return_value = '{"recommended_method": "failover", "confidence": 80, "data_loss_risk": "low", "estimated_data_loss_ms": 50, "warnings": []}'

        result = advise_aurora_promotion(
            _make_stability_context(), "region_failure", mode="autonomous"
        )

        assert result["should_auto_execute"] is True

    @patch("ai.aurora_advisor.call_llm")
    def test_guardrails_override_llm(self, mock_llm):
        from ai.aurora_advisor import advise_aurora_promotion

        mock_llm.return_value = '{"recommended_method": "switchover", "confidence": 99, "data_loss_risk": "none", "estimated_data_loss_ms": 0, "warnings": []}'

        # Bad instance state should block
        ctx = _make_stability_context()
        ctx["aurora_instance_status"]["instances"][0]["status"] = "rebooting"

        result = advise_aurora_promotion(ctx, "app_failure", mode="autonomous")

        assert result["guardrails_passed"] is False
        assert result["should_auto_execute"] is False
        assert len(result["guardrail_reasons"]) > 0

    @patch("ai.aurora_advisor.call_llm")
    def test_llm_error_returns_fallback(self, mock_llm):
        from ai.aurora_advisor import advise_aurora_promotion

        mock_llm.return_value = "[LLM] Call failed: timeout"

        result = advise_aurora_promotion(
            _make_stability_context(), "app_failure", mode="advisory"
        )

        assert result["should_auto_execute"] is False
        assert result["confidence"] == 0
        assert "unavailable" in result["reasoning"]

    @patch("ai.aurora_advisor.call_llm", side_effect=Exception("boom"))
    def test_exception_returns_fallback(self, mock_llm):
        from ai.aurora_advisor import advise_aurora_promotion

        result = advise_aurora_promotion(
            _make_stability_context(), "region_failure", mode="guided"
        )

        assert result["should_auto_execute"] is False
        assert result["recommended_method"] == "failover"


# ===========================================================================
# SNS Formatting Tests
# ===========================================================================


class TestFormatAdvisorForSNS:
    """Tests for SNS formatting."""

    def test_formats_advisory_recommendation(self):
        from ai.aurora_advisor import format_advisor_for_sns

        rec = {
            "recommended_method": "switchover",
            "confidence": 92,
            "data_loss_risk": "none",
            "estimated_data_loss_ms": 0,
            "reasoning": "Lag is stable.",
            "warnings": [],
            "should_auto_execute": False,
            "guardrails_passed": True,
            "guardrail_reasons": [],
        }

        formatted = format_advisor_for_sns(rec, _make_stability_context())

        assert "AURORA PROMOTION ADVISOR" in formatted
        assert "SWITCHOVER" in formatted
        assert "92%" in formatted
        assert "Manual promotion required" in formatted

    def test_formats_auto_execute(self):
        from ai.aurora_advisor import format_advisor_for_sns

        rec = {
            "recommended_method": "switchover",
            "confidence": 95,
            "data_loss_risk": "none",
            "estimated_data_loss_ms": 0,
            "reasoning": "All good.",
            "warnings": [],
            "should_auto_execute": True,
            "guardrails_passed": True,
            "guardrail_reasons": [],
        }

        formatted = format_advisor_for_sns(rec, _make_stability_context())
        assert "Auto-executing" in formatted

    def test_formats_guardrail_block(self):
        from ai.aurora_advisor import format_advisor_for_sns

        rec = {
            "recommended_method": "switchover",
            "confidence": 95,
            "data_loss_risk": "none",
            "estimated_data_loss_ms": 0,
            "reasoning": "Looks good but guardrails say no.",
            "warnings": [],
            "should_auto_execute": False,
            "guardrails_passed": False,
            "guardrail_reasons": ["Replication lag too high", "Instance rebooting"],
        }

        formatted = format_advisor_for_sns(rec, _make_stability_context())
        assert "GUARDRAILS BLOCKED" in formatted
        assert "Replication lag too high" in formatted

    def test_formats_with_warnings_and_data_loss(self):
        from ai.aurora_advisor import format_advisor_for_sns

        rec = {
            "recommended_method": "failover",
            "confidence": 70,
            "data_loss_risk": "medium",
            "estimated_data_loss_ms": 500,
            "reasoning": "Region is down.",
            "warnings": ["Primary unreachable"],
            "should_auto_execute": False,
            "guardrails_passed": True,
            "guardrail_reasons": [],
        }

        formatted = format_advisor_for_sns(rec, _make_stability_context())
        assert "FAILOVER" in formatted
        assert "500ms" in formatted
        assert "Primary unreachable" in formatted


# ===========================================================================
# Config Tests
# ===========================================================================


class TestAuroraAdvisorConfig:
    """Tests for aurora advisor config."""

    def test_defaults(self):
        import importlib
        import ai.config
        importlib.reload(ai.config)

        assert ai.config.AI_AURORA_ADVISOR_MODE == "disabled"
        assert ai.config.AI_AURORA_ADVISOR_CONFIDENCE_THRESHOLD == 90
        assert ai.config.AI_AURORA_ADVISOR_MAX_LAG_MS == 100

    def test_custom_mode(self):
        with patch.dict(os.environ, {"AI_AURORA_ADVISOR_MODE": "guided"}):
            import importlib
            import ai.config
            importlib.reload(ai.config)
            assert ai.config.AI_AURORA_ADVISOR_MODE == "guided"
