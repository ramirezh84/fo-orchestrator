#!/usr/bin/env python3
"""
Tests for the AI failback readiness assessment module.

Run: python3 -m pytest tests/test_failback_readiness.py -v
"""

import json
import os
from unittest.mock import patch

import pytest

os.environ.setdefault("SNS_TOPIC_ARN", "arn:aws:sns:us-east-1:123456789:test")
os.environ.setdefault("AI_RCA_ENABLED", "false")

SAMPLE_STABILITY_CONTEXT = {
    "region": "us-west-1",
    "window_minutes": 15,
    "aurora_replication_lag": {
        "replica_count": 1,
        "replicas": {
            "reader-1": {
                "summary": {"min": 2.0, "max": 5.0, "avg": 3.5, "latest": 3.0, "datapoint_count": 15},
            }
        },
    },
    "aurora_cluster_detail": {"status": "available", "is_writer": True},
    "aurora_instance_status": {"instances": [
        {"instance_id": "writer-1", "is_writer": True, "status": "available"},
    ]},
    "aurora_global_topology": {"status": "available", "members": []},
    "aurora_events": {"events": []},
    "ecs_task_stability": {"running_count_trend": {"summary": {"avg": 4.0}}, "recently_stopped_tasks": 0},
    "alb_error_trend": {"5xx_count": {"summary": {"avg": 0.0}}},
}


class TestParseReadinessResponse:
    """Tests for LLM response parsing."""

    def test_parse_go_verdict(self):
        from ai.failback_readiness import _parse_readiness_response

        llm_text = """```json
{"verdict": "GO", "confidence": 95, "recommended_wait_minutes": 0, "risks": []}
```

REASONING:
All metrics are stable. Aurora replication lag has been under 5ms for 15 minutes."""

        result = _parse_readiness_response(llm_text)
        assert result["verdict"] == "GO"
        assert result["confidence"] == 95
        assert result["recommended_wait_minutes"] == 0
        assert "stable" in result["reasoning"].lower()

    def test_parse_no_go_verdict(self):
        from ai.failback_readiness import _parse_readiness_response

        llm_text = '{"verdict": "NO_GO", "confidence": 80, "recommended_wait_minutes": 10, "risks": ["Replication lag spiking"]}\n\nREASONING:\nLag is unstable.'

        result = _parse_readiness_response(llm_text)
        assert result["verdict"] == "NO_GO"
        assert result["confidence"] == 80
        assert result["recommended_wait_minutes"] == 10
        assert len(result["risks"]) == 1

    def test_parse_caution_verdict(self):
        from ai.failback_readiness import _parse_readiness_response

        llm_text = '{"verdict": "CAUTION", "confidence": 60, "recommended_wait_minutes": 5, "risks": ["Minor ECS task restarts"]}'

        result = _parse_readiness_response(llm_text)
        assert result["verdict"] == "CAUTION"
        assert result["confidence"] == 60

    def test_fallback_on_invalid_json(self):
        from ai.failback_readiness import _parse_readiness_response

        result = _parse_readiness_response("This is not JSON at all, just plain analysis text.")
        assert result["verdict"] == "CAUTION"
        assert result["confidence"] == 50
        assert "not JSON" in result["raw_analysis"]

    def test_fallback_on_missing_verdict(self):
        from ai.failback_readiness import _parse_readiness_response

        result = _parse_readiness_response('{"confidence": 90}')
        # No verdict key -> stays CAUTION default
        assert result["verdict"] == "CAUTION"

    def test_clamps_confidence_range(self):
        from ai.failback_readiness import _parse_readiness_response

        result = _parse_readiness_response('{"verdict": "GO", "confidence": 150}')
        assert result["confidence"] == 100

        result = _parse_readiness_response('{"verdict": "GO", "confidence": -10}')
        assert result["confidence"] == 0

    def test_preserves_raw_analysis(self):
        from ai.failback_readiness import _parse_readiness_response

        llm_text = '{"verdict": "GO", "confidence": 90}\n\nSome detailed analysis here.'
        result = _parse_readiness_response(llm_text)
        assert result["raw_analysis"] == llm_text


class TestAssessFailbackReadiness:
    """Tests for the main assessment function."""

    @patch("ai.failback_readiness.call_llm")
    def test_success_returns_parsed_result(self, mock_llm):
        from ai.failback_readiness import assess_failback_readiness

        mock_llm.return_value = '{"verdict": "GO", "confidence": 92, "recommended_wait_minutes": 0, "risks": []}\n\nREASONING:\nAll stable.'

        result = assess_failback_readiness(SAMPLE_STABILITY_CONTEXT)

        assert result["verdict"] == "GO"
        assert result["confidence"] == 92
        mock_llm.assert_called_once()

    @patch("ai.failback_readiness.call_llm")
    def test_llm_error_returns_caution(self, mock_llm):
        from ai.failback_readiness import assess_failback_readiness

        mock_llm.return_value = "[LLM] Call failed: timeout"

        result = assess_failback_readiness(SAMPLE_STABILITY_CONTEXT)

        assert result["verdict"] == "CAUTION"
        assert result["confidence"] == 0
        assert "unavailable" in result["reasoning"]

    @patch("ai.failback_readiness.call_llm", side_effect=Exception("unexpected"))
    def test_exception_returns_caution(self, mock_llm):
        from ai.failback_readiness import assess_failback_readiness

        result = assess_failback_readiness(SAMPLE_STABILITY_CONTEXT)

        assert result["verdict"] == "CAUTION"
        assert result["confidence"] == 0

    @patch("ai.failback_readiness.call_llm")
    def test_passes_region_to_llm(self, mock_llm):
        from ai.failback_readiness import assess_failback_readiness

        mock_llm.return_value = '{"verdict": "GO", "confidence": 90}'

        assess_failback_readiness(SAMPLE_STABILITY_CONTEXT, region="us-west-1")

        _, kwargs = mock_llm.call_args
        assert kwargs.get("region") == "us-west-1" or mock_llm.call_args[0][1] == "us-west-1"


class TestBuildFailbackPrompt:
    """Tests for prompt construction."""

    def test_includes_all_context_fields(self):
        from ai.failback_readiness import _build_failback_prompt

        prompt = _build_failback_prompt(SAMPLE_STABILITY_CONTEXT)

        assert "us-west-1" in prompt
        assert "15 minutes" in prompt
        assert "reader-1" in prompt
        assert "available" in prompt
        assert "GO" in prompt  # from instructions
        assert "NO_GO" in prompt
        assert "CAUTION" in prompt

    def test_handles_missing_fields(self):
        from ai.failback_readiness import _build_failback_prompt

        prompt = _build_failback_prompt({"region": "us-east-1", "window_minutes": 5})

        assert "us-east-1" in prompt
        assert "N/A" in prompt


class TestFormatReadinessForSNS:
    """Tests for SNS formatting."""

    def test_formats_go_verdict(self):
        from ai.failback_readiness import format_readiness_for_sns

        assessment = {
            "verdict": "GO",
            "confidence": 95,
            "reasoning": "All stable.",
            "risks": [],
            "recommended_wait_minutes": 0,
        }

        formatted = format_readiness_for_sns(assessment, SAMPLE_STABILITY_CONTEXT)

        assert "FAILBACK READINESS ASSESSMENT" in formatted
        assert "GO" in formatted
        assert "95%" in formatted
        assert "All stable." in formatted
        assert "AI-generated" in formatted

    def test_formats_no_go_with_risks_and_wait(self):
        from ai.failback_readiness import format_readiness_for_sns

        assessment = {
            "verdict": "NO_GO",
            "confidence": 80,
            "reasoning": "Lag is unstable.",
            "risks": ["Replication lag spiking", "ECS tasks restarting"],
            "recommended_wait_minutes": 10,
        }

        formatted = format_readiness_for_sns(assessment, SAMPLE_STABILITY_CONTEXT)

        assert "NO GO" in formatted
        assert "Replication lag spiking" in formatted
        assert "ECS tasks restarting" in formatted
        assert "10 more minutes" in formatted

    def test_includes_provider_info(self):
        from ai.failback_readiness import format_readiness_for_sns

        assessment = {"verdict": "GO", "confidence": 90, "reasoning": "ok", "risks": [], "recommended_wait_minutes": 0}

        formatted = format_readiness_for_sns(assessment, SAMPLE_STABILITY_CONTEXT)

        assert "us-west-1" in formatted
        assert "15m" in formatted


class TestFailbackReadinessConfig:
    """Tests for failback readiness config."""

    def test_defaults(self):
        import importlib
        import ai.config
        importlib.reload(ai.config)

        assert ai.config.AI_FAILBACK_READINESS_ENABLED is False
        assert ai.config.AI_FAILBACK_STABILITY_WINDOW_MINUTES == 15

    def test_enabled(self):
        with patch.dict(os.environ, {"AI_FAILBACK_READINESS_ENABLED": "true"}):
            import importlib
            import ai.config
            importlib.reload(ai.config)
            assert ai.config.AI_FAILBACK_READINESS_ENABLED is True
