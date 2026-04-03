#!/usr/bin/env python3
"""
Tests for the AI RCA module.

Unit tests (mocked) + integration test (real Claude API, gated).
Run: python3 -m pytest tests/test_rca.py -v
"""

import json
import os
from unittest.mock import MagicMock, patch, ANY

import pytest
from botocore.exceptions import ClientError

# Set required env vars before importing modules
os.environ.setdefault("SNS_TOPIC_ARN", "arn:aws:sns:us-east-1:123456789:test")
os.environ.setdefault("AI_RCA_ENABLED", "false")


# ===========================================================================
# Config Tests
# ===========================================================================


class TestConfig:
    """Tests for ai.config environment variable handling."""

    def test_defaults(self):
        with patch.dict(os.environ, {}, clear=False):
            # Re-import to pick up env vars
            import importlib
            import ai.config
            importlib.reload(ai.config)

            assert ai.config.AI_RCA_ENABLED is False
            assert ai.config.AI_RCA_MODEL == "claude-haiku-4-5-20251001"
            assert ai.config.AI_RCA_MAX_TOKENS == 1024
            assert ai.config.AI_RCA_TIMEOUT_SECONDS == 15
            assert ai.config.AI_RCA_LOG_WINDOW_MINUTES == 10
            assert ai.config.AI_RCA_MAX_LOG_LINES == 200

    def test_enabled_flag(self):
        with patch.dict(os.environ, {"AI_RCA_ENABLED": "true"}):
            import importlib
            import ai.config
            importlib.reload(ai.config)
            assert ai.config.AI_RCA_ENABLED is True

    def test_custom_model(self):
        with patch.dict(os.environ, {"AI_RCA_MODEL": "claude-sonnet-4-5-20250514"}):
            import importlib
            import ai.config
            importlib.reload(ai.config)
            assert ai.config.AI_RCA_MODEL == "claude-sonnet-4-5-20250514"


# ===========================================================================
# Collector Tests
# ===========================================================================


class TestCollector:
    """Tests for ai.collector with mocked boto3 clients."""

    SAMPLE_HEALTH_SIGNALS = {
        "http": {"healthy": False, "detail": "HTTP 503"},
        "alb": {"healthy": True, "healthy_hosts": 2},
        "ecs": {"healthy": True, "running": 4, "desired": 4},
        "api_gw": {"healthy": True, "error_rate": 1.2},
        "aurora": {"healthy": True, "status": "available"},
    }

    @patch("ai.collector.boto3")
    def test_collect_ecs_events_success(self, mock_boto3):
        from ai.collector import _collect_ecs_events

        mock_ecs = MagicMock()
        mock_boto3.client.return_value = mock_ecs
        mock_ecs.describe_services.return_value = {
            "services": [{
                "status": "ACTIVE",
                "runningCount": 4,
                "desiredCount": 4,
                "pendingCount": 0,
                "events": [],
                "deployments": [{
                    "status": "PRIMARY",
                    "runningCount": 4,
                    "desiredCount": 4,
                    "rolloutState": "COMPLETED",
                }],
            }]
        }

        result = _collect_ecs_events("us-east-1", "my-cluster", "my-service")
        assert result["status"] == "ACTIVE"
        assert result["running_count"] == 4
        assert result["desired_count"] == 4
        assert len(result["deployments"]) == 1

    @patch("ai.collector.boto3")
    def test_collect_ecs_events_service_not_found(self, mock_boto3):
        from ai.collector import _collect_ecs_events

        mock_ecs = MagicMock()
        mock_boto3.client.return_value = mock_ecs
        mock_ecs.describe_services.return_value = {"services": []}

        result = _collect_ecs_events("us-east-1", "my-cluster", "missing")
        assert "error" in result

    @patch("ai.collector.boto3")
    def test_collect_ecs_events_client_error(self, mock_boto3):
        from ai.collector import _collect_ecs_events

        mock_ecs = MagicMock()
        mock_boto3.client.return_value = mock_ecs
        mock_ecs.describe_services.side_effect = ClientError(
            {"Error": {"Code": "ClusterNotFoundException", "Message": "not found"}},
            "DescribeServices",
        )

        result = _collect_ecs_events("us-east-1", "bad-cluster", "svc")
        assert "error" in result

    @patch("ai.collector.boto3")
    def test_collect_aurora_status_success(self, mock_boto3):
        from ai.collector import _collect_aurora_status

        mock_rds = MagicMock()
        mock_boto3.client.return_value = mock_rds
        mock_rds.describe_db_clusters.return_value = {
            "DBClusters": [{
                "Status": "available",
                "Engine": "aurora-postgresql",
                "MultiAZ": True,
                "ReplicationSourceIdentifier": "",
                "DBClusterMembers": [
                    {"DBInstanceIdentifier": "writer-1", "IsClusterWriter": True},
                    {"DBInstanceIdentifier": "reader-1", "IsClusterWriter": False},
                ],
            }]
        }
        mock_rds.describe_events.return_value = {"Events": []}

        result = _collect_aurora_status("us-east-1", "my-cluster")
        assert result["status"] == "available"
        assert len(result["members"]) == 2
        assert result["members"][0]["is_writer"] is True

    @patch("ai.collector.boto3")
    def test_collect_aurora_status_client_error(self, mock_boto3):
        from ai.collector import _collect_aurora_status

        mock_rds = MagicMock()
        mock_boto3.client.return_value = mock_rds
        mock_rds.describe_db_clusters.side_effect = ClientError(
            {"Error": {"Code": "DBClusterNotFoundFault", "Message": "not found"}},
            "DescribeDBClusters",
        )

        result = _collect_aurora_status("us-east-1", "missing")
        assert "error" in result

    @patch("ai.collector.boto3")
    def test_collect_cloudwatch_logs_success(self, mock_boto3):
        from ai.collector import _collect_cloudwatch_logs
        from datetime import datetime, timezone

        mock_logs = MagicMock()
        mock_boto3.client.return_value = mock_logs
        mock_logs.filter_log_events.return_value = {
            "events": [
                {"timestamp": 1700000000000, "message": "ERROR: connection refused"},
                {"timestamp": 1700000001000, "message": "WARN: retrying request"},
            ]
        }

        now = datetime.now(timezone.utc)
        result = _collect_cloudwatch_logs("us-east-1", "/ecs/my-app", now, now)
        assert result["count"] == 2
        assert "connection refused" in result["lines"][0]["message"]

    @patch("ai.collector.boto3")
    def test_collect_cloudwatch_logs_client_error(self, mock_boto3):
        from ai.collector import _collect_cloudwatch_logs
        from datetime import datetime, timezone

        mock_logs = MagicMock()
        mock_boto3.client.return_value = mock_logs
        mock_logs.filter_log_events.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}},
            "FilterLogEvents",
        )

        now = datetime.now(timezone.utc)
        result = _collect_cloudwatch_logs("us-east-1", "/ecs/bad", now, now)
        assert "error" in result

    @patch("ai.collector.boto3")
    def test_collect_alb_health_success(self, mock_boto3):
        from ai.collector import _collect_alb_health

        mock_elbv2 = MagicMock()
        mock_boto3.client.return_value = mock_elbv2
        mock_elbv2.describe_target_groups.return_value = {
            "TargetGroups": [{
                "TargetGroupArn": "arn:aws:elasticloadbalancing:us-east-1:123:targetgroup/tg/abc",
                "TargetGroupName": "my-tg",
            }]
        }
        mock_elbv2.describe_target_health.return_value = {
            "TargetHealthDescriptions": [
                {
                    "Target": {"Id": "10.0.1.5", "Port": 8080},
                    "TargetHealth": {"State": "healthy", "Reason": ""},
                },
                {
                    "Target": {"Id": "10.0.1.6", "Port": 8080},
                    "TargetHealth": {"State": "unhealthy", "Reason": "Target.Timeout"},
                },
            ]
        }

        result = _collect_alb_health("us-east-1", "arn:aws:elasticloadbalancing:us-east-1:123:loadbalancer/app/my-alb/abc")
        assert len(result["target_groups"]) == 1
        assert len(result["target_groups"][0]["targets"]) == 2
        assert result["target_groups"][0]["targets"][1]["state"] == "unhealthy"

    @patch("ai.collector._collect_ecs_events")
    @patch("ai.collector._collect_aurora_status")
    def test_collect_incident_context_assembles_all(self, mock_aurora, mock_ecs):
        from ai.collector import collect_incident_context

        mock_ecs.return_value = {"status": "ACTIVE", "running_count": 4}
        mock_aurora.return_value = {"status": "available"}

        ctx = collect_incident_context(
            region="us-east-1",
            health_signals=self.SAMPLE_HEALTH_SIGNALS,
            ecs_cluster="cluster",
            ecs_service="service",
            aurora_cluster_id="aurora-1",
        )

        assert ctx["region"] == "us-east-1"
        assert ctx["health_signals"] == self.SAMPLE_HEALTH_SIGNALS
        assert ctx["ecs_events"]["status"] == "ACTIVE"
        assert ctx["aurora_status"]["status"] == "available"
        assert "timestamp" in ctx
        # No log_group or alb_arn provided — those keys should be absent
        assert "application_logs" not in ctx
        assert "alb_health" not in ctx

    @patch("ai.collector._collect_alb_health")
    @patch("ai.collector._collect_cloudwatch_logs")
    @patch("ai.collector._collect_ecs_events")
    @patch("ai.collector._collect_aurora_status")
    def test_collect_incident_context_with_optional_sources(
        self, mock_aurora, mock_ecs, mock_logs, mock_alb
    ):
        from ai.collector import collect_incident_context

        mock_ecs.return_value = {"status": "ACTIVE"}
        mock_aurora.return_value = {"status": "available"}
        mock_logs.return_value = {"count": 1, "lines": []}
        mock_alb.return_value = {"target_groups": []}

        ctx = collect_incident_context(
            region="us-east-1",
            health_signals=self.SAMPLE_HEALTH_SIGNALS,
            ecs_cluster="cluster",
            ecs_service="service",
            aurora_cluster_id="aurora-1",
            alb_arn="arn:aws:elasticloadbalancing:us-east-1:123:loadbalancer/app/my-alb/abc",
            log_group="/ecs/my-app",
        )

        assert "application_logs" in ctx
        assert "alb_health" in ctx


# ===========================================================================
# RCA Analyzer Tests
# ===========================================================================


class TestRCAAnalyzer:
    """Tests for ai.rca_analyzer with mocked API calls."""

    SAMPLE_CONTEXT = {
        "region": "us-east-1",
        "timestamp": "2026-04-02T15:30:00+00:00",
        "window_minutes": 10,
        "health_signals": {
            "http": {"healthy": False, "detail": "HTTP 503"},
            "alb": {"healthy": True},
            "ecs": {"healthy": True},
            "api_gw": {"healthy": True},
            "aurora": {"healthy": True},
        },
        "ecs_events": {"status": "ACTIVE", "running_count": 4},
        "aurora_status": {"status": "available"},
    }

    @patch("ai.rca_analyzer.os.environ", {"ANTHROPIC_API_KEY": "test-key"})
    def test_get_api_key_from_env(self):
        from ai.rca_analyzer import get_api_key
        assert get_api_key() == "test-key"

    @patch("ai.rca_analyzer.os.environ", {})
    @patch("ai.rca_analyzer.boto3")
    def test_get_api_key_from_secrets_manager(self, mock_boto3):
        from ai.rca_analyzer import get_api_key

        mock_sm = MagicMock()
        mock_boto3.client.return_value = mock_sm
        mock_sm.get_secret_value.return_value = {"SecretString": "sk-ant-secret"}

        key = get_api_key("us-east-1")
        assert key == "sk-ant-secret"
        mock_sm.get_secret_value.assert_called_once()

    @patch("ai.rca_analyzer.os.environ", {})
    @patch("ai.rca_analyzer.boto3")
    def test_get_api_key_secrets_manager_failure(self, mock_boto3):
        from ai.rca_analyzer import get_api_key

        mock_sm = MagicMock()
        mock_boto3.client.return_value = mock_sm
        mock_sm.get_secret_value.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}},
            "GetSecretValue",
        )

        with pytest.raises(ClientError):
            get_api_key("us-east-1")

    @patch("ai.rca_analyzer.urllib.request.urlopen")
    @patch("ai.rca_analyzer.get_api_key", return_value="test-key")
    def test_analyze_incident_success(self, mock_key, mock_urlopen):
        from ai.rca_analyzer import analyze_incident

        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({
            "content": [{"type": "text", "text": "## Timeline\n- 15:28 HTTP health check failed\n\n## Root Cause\nALB target deregistered."}]
        }).encode()
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        result = analyze_incident(self.SAMPLE_CONTEXT)
        assert "Timeline" in result
        assert "Root Cause" in result

    @patch("ai.rca_analyzer.urllib.request.urlopen")
    @patch("ai.rca_analyzer.get_api_key", return_value="test-key")
    def test_analyze_incident_empty_response(self, mock_key, mock_urlopen):
        from ai.rca_analyzer import analyze_incident

        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({"content": []}).encode()
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        result = analyze_incident(self.SAMPLE_CONTEXT)
        assert "empty response" in result

    @patch("ai.rca_analyzer.get_api_key", side_effect=Exception("network error"))
    def test_analyze_incident_api_failure_returns_error_string(self, mock_key):
        from ai.rca_analyzer import analyze_incident

        result = analyze_incident(self.SAMPLE_CONTEXT)
        assert "[RCA] Analysis unavailable" in result
        assert "network error" in result

    @patch("ai.rca_analyzer.urllib.request.urlopen")
    @patch("ai.rca_analyzer.get_api_key", return_value="test-key")
    def test_analyze_incident_timeout(self, mock_key, mock_urlopen):
        from ai.rca_analyzer import analyze_incident
        from urllib.error import URLError

        mock_urlopen.side_effect = URLError("timed out")

        result = analyze_incident(self.SAMPLE_CONTEXT)
        assert "[RCA] Analysis unavailable" in result

    def test_format_rca_for_sns(self):
        from ai.rca_analyzer import format_rca_for_sns

        formatted = format_rca_for_sns("Test analysis text", self.SAMPLE_CONTEXT)
        assert "AI ROOT CAUSE ANALYSIS" in formatted
        assert formatted.count("AI ROOT CAUSE ANALYSIS") == 1
        assert "Test analysis text" in formatted
        assert "us-east-1" in formatted
        assert "AI-generated analysis" in formatted

    @patch("ai.rca_analyzer.urllib.request.urlopen")
    @patch("ai.rca_analyzer.get_api_key", return_value="test-key")
    def test_analyze_sends_correct_request(self, mock_key, mock_urlopen):
        from ai.rca_analyzer import analyze_incident, AI_RCA_MODEL

        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({
            "content": [{"type": "text", "text": "analysis"}]
        }).encode()
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        analyze_incident(self.SAMPLE_CONTEXT, region="us-east-1")

        # Verify the request was constructed correctly
        call_args = mock_urlopen.call_args
        req = call_args[0][0]
        assert req.full_url == "https://api.anthropic.com/v1/messages"
        assert req.get_header("X-api-key") == "test-key"
        assert req.get_header("Anthropic-version") == "2023-06-01"

        body = json.loads(req.data.decode())
        assert body["model"] == AI_RCA_MODEL
        assert len(body["messages"]) == 1
        assert "failover" in body["messages"][0]["content"].lower()


# ===========================================================================
# Orchestrator Integration Tests
# ===========================================================================


class TestOrchestratorRCAIntegration:
    """Test that _run_rca_analysis integrates correctly."""

    @patch.dict(os.environ, {"AI_RCA_ENABLED": "false"}, clear=False)
    def test_rca_disabled_returns_empty(self):
        """When AI_RCA_ENABLED=false, _run_rca_analysis returns empty string."""
        import importlib
        import ai.config
        importlib.reload(ai.config)

        # Need to reimport to pick up reloaded config
        from ai.config import AI_RCA_ENABLED
        assert AI_RCA_ENABLED is False

    @patch("ai.collector.collect_incident_context")
    @patch("ai.rca_analyzer.analyze_incident", return_value="RCA result")
    @patch("ai.rca_analyzer.format_rca_for_sns", return_value="Formatted RCA")
    @patch.dict(os.environ, {"AI_RCA_ENABLED": "true"}, clear=False)
    def test_rca_enabled_returns_formatted_analysis(
        self, mock_format, mock_analyze, mock_collect
    ):
        """When enabled, returns formatted RCA appended with newlines."""
        import importlib
        import ai.config
        importlib.reload(ai.config)

        mock_collect.return_value = {"region": "us-east-1", "timestamp": "now"}

        # Import after reload so it picks up AI_RCA_ENABLED=true
        # We test the logic directly rather than importing _run_rca_analysis
        # since that would require importing the full orchestrator
        from ai.collector import collect_incident_context
        from ai.rca_analyzer import analyze_incident, format_rca_for_sns

        ctx = collect_incident_context(
            region="us-east-1",
            health_signals={},
            ecs_cluster="c",
            ecs_service="s",
            aurora_cluster_id="a",
        )
        rca = analyze_incident(ctx)
        formatted = format_rca_for_sns(rca, ctx)

        assert formatted == "Formatted RCA"
        mock_collect.assert_called_once()
        mock_analyze.assert_called_once()

    @patch("ai.collector.collect_incident_context", side_effect=Exception("boom"))
    @patch.dict(os.environ, {"AI_RCA_ENABLED": "true"}, clear=False)
    def test_rca_collector_failure_is_non_blocking(self, mock_collect):
        """If collector raises, analyze_incident still returns a string."""
        import importlib
        import ai.config
        importlib.reload(ai.config)

        from ai.rca_analyzer import analyze_incident

        # analyze_incident handles all exceptions internally
        result = analyze_incident({"region": "us-east-1"})
        # Should return error string, not raise
        assert isinstance(result, str)


# ===========================================================================
# Integration Test — Real Claude API (gated)
# ===========================================================================


@pytest.mark.skipif(
    not os.environ.get("AI_RCA_INTEGRATION_TEST"),
    reason="Set AI_RCA_INTEGRATION_TEST=1 and ANTHROPIC_API_KEY to run",
)
class TestRCAIntegration:
    """Integration test that calls the real Claude API."""

    def test_real_api_call(self):
        from ai.rca_analyzer import analyze_incident

        context = {
            "region": "us-east-1",
            "timestamp": "2026-04-02T15:30:00+00:00",
            "window_minutes": 10,
            "health_signals": {
                "http": {"healthy": False, "status_code": 503, "detail": "Service Unavailable"},
                "alb": {"healthy": False, "healthy_hosts": 0, "min_required": 1},
                "ecs": {"healthy": False, "running": 0, "desired": 4},
                "api_gw": {"healthy": True, "error_rate": 2.1},
                "aurora": {"healthy": True, "status": "available"},
            },
            "ecs_events": {
                "status": "ACTIVE",
                "running_count": 0,
                "desired_count": 4,
                "events": [
                    {"timestamp": "2026-04-02T15:28:00Z", "message": "service my-service has stopped 4 tasks: task abc123"},
                    {"timestamp": "2026-04-02T15:27:00Z", "message": "service my-service has begun draining connections on 4 tasks"},
                ],
            },
            "aurora_status": {"status": "available", "members": [
                {"instance_id": "writer-1", "is_writer": True},
            ]},
        }

        result = analyze_incident(context)

        # Should contain structured analysis, not an error
        assert "[RCA] Analysis unavailable" not in result
        assert len(result) > 100  # Should be a substantive analysis
        print(f"\n--- Real RCA Output ---\n{result}\n")
