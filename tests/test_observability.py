#!/usr/bin/env python3
"""Unit tests for observability.py — the metric-emission helpers shared by
the orchestrator and failback Lambdas (issue #98)."""

import os
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(autouse=True)
def _clear_cw_client_cache():
    """observability caches CW clients per region. Clear between tests so
    each test starts fresh and patches don't leak."""
    import observability
    observability._cw_clients.clear()
    yield
    observability._cw_clients.clear()


@pytest.fixture
def mock_cw():
    """Patch boto3.client('cloudwatch') and return the MagicMock so the test
    can assert against put_metric_data calls."""
    with patch("observability.boto3.client") as mock_factory:
        client = MagicMock()
        mock_factory.return_value = client
        yield client


def _put_calls(mock_cw):
    """All MetricData lists passed to put_metric_data, flattened."""
    return [c.kwargs.get("MetricData") or c[1].get("MetricData")
            for c in mock_cw.put_metric_data.call_args_list]


def _all_dims(metric):
    """Return {dim_name: dim_value} for one metric datapoint."""
    return {d["Name"]: d["Value"] for d in metric["Dimensions"]}


# ---------------------------------------------------------------------------
# publish_state_metrics
# ---------------------------------------------------------------------------

class TestPublishStateMetrics:
    def test_emits_4_metrics_in_one_batched_put(self, mock_cw):
        import observability
        state = {
            "latch_engaged": True,
            "aurora_promotion_pending": False,
            "redis_promotion_pending": True,
            "consecutive_failures": 2,
        }
        observability.publish_state_metrics(state, "us-east-1")
        assert mock_cw.put_metric_data.call_count == 1
        metric_data = _put_calls(mock_cw)[0]
        assert {m["MetricName"] for m in metric_data} == {
            "LatchEngaged", "AuroraPromotionPending",
            "RedisPromotionPending", "ConsecutiveFailures",
        }

    def test_values_match_state_fields(self, mock_cw):
        import observability
        observability.publish_state_metrics({
            "latch_engaged": True,
            "aurora_promotion_pending": True,
            "redis_promotion_pending": False,
            "consecutive_failures": 5,
        }, "us-east-1")
        by_name = {m["MetricName"]: m for m in _put_calls(mock_cw)[0]}
        assert by_name["LatchEngaged"]["Value"] == 1.0
        assert by_name["AuroraPromotionPending"]["Value"] == 1.0
        assert by_name["RedisPromotionPending"]["Value"] == 0.0
        assert by_name["ConsecutiveFailures"]["Value"] == 5.0
        assert by_name["LatchEngaged"]["Unit"] == "None"
        assert by_name["ConsecutiveFailures"]["Unit"] == "Count"

    def test_attaches_three_dimensions_with_env_values(self, mock_cw):
        import observability
        with patch.dict(os.environ, {
            "APP_NAME": "fo-v16-drill",
            "ROUTING_MODE": "active-active",
        }):
            observability.publish_state_metrics({}, "us-west-2")
        dims = _all_dims(_put_calls(mock_cw)[0][0])
        assert dims == {
            "Region": "us-west-2",
            "AppName": "fo-v16-drill",
            "RoutingMode": "active-active",
        }

    def test_app_name_defaults_when_env_unset(self, mock_cw):
        import observability
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("APP_NAME", None)
            observability.publish_state_metrics({}, "us-east-1")
        dims = _all_dims(_put_calls(mock_cw)[0][0])
        assert dims["AppName"] == "(unset)"

    def test_routing_mode_defaults_to_failover(self, mock_cw):
        import observability
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ROUTING_MODE", None)
            observability.publish_state_metrics({}, "us-east-1")
        dims = _all_dims(_put_calls(mock_cw)[0][0])
        assert dims["RoutingMode"] == "failover"

    def test_swallows_cw_exception(self, mock_cw, caplog):
        import observability
        mock_cw.put_metric_data.side_effect = RuntimeError("CW down")
        # Must not raise.
        observability.publish_state_metrics({"latch_engaged": False}, "us-east-1")
        assert "put_metric_data failed" in caplog.text

    def test_handles_non_dict_state_silently(self, mock_cw):
        import observability
        observability.publish_state_metrics(None, "us-east-1")
        observability.publish_state_metrics("not a dict", "us-east-1")
        assert mock_cw.put_metric_data.call_count == 0

    def test_handles_missing_state_keys_as_falsy(self, mock_cw):
        import observability
        observability.publish_state_metrics({}, "us-east-1")
        by_name = {m["MetricName"]: m for m in _put_calls(mock_cw)[0]}
        assert by_name["LatchEngaged"]["Value"] == 0.0
        assert by_name["AuroraPromotionPending"]["Value"] == 0.0
        assert by_name["RedisPromotionPending"]["Value"] == 0.0
        assert by_name["ConsecutiveFailures"]["Value"] == 0.0


# ---------------------------------------------------------------------------
# publish_signal_metrics
# ---------------------------------------------------------------------------

class TestPublishSignalMetrics:
    def test_emits_one_metric_per_non_skipped_signal(self, mock_cw):
        import observability
        health = {
            "healthy": False,
            "signals": [
                {"signal": "http_health",       "healthy": False},
                {"signal": "alb_healthy_hosts", "healthy": True},
                {"signal": "ecs_running_tasks", "healthy": True},
                {"signal": "aurora_status",     "healthy": True},
            ],
        }
        observability.publish_signal_metrics(health, "us-east-1")
        assert mock_cw.put_metric_data.call_count == 1
        metric_data = _put_calls(mock_cw)[0]
        assert {m["MetricName"] for m in metric_data} == {
            "SignalHttp", "SignalAlb", "SignalEcs", "SignalAurora",
        }
        by_name = {m["MetricName"]: m for m in metric_data}
        assert by_name["SignalHttp"]["Value"] == 0.0
        assert by_name["SignalAlb"]["Value"] == 1.0

    def test_skipped_signals_are_omitted(self, mock_cw):
        import observability
        health = {
            "signals": [
                {"signal": "http_health",        "healthy": True},
                {"signal": "elasticache_status", "skipped": True,
                 "reason": "Not configured"},
                {"signal": "api_gw_5xx",         "skipped": True,
                 "reason": "Not configured"},
            ],
        }
        observability.publish_signal_metrics(health, "us-east-1")
        metric_data = _put_calls(mock_cw)[0]
        names = {m["MetricName"] for m in metric_data}
        assert "SignalElasticache" not in names
        assert "SignalApiGw" not in names
        assert "SignalHttp" in names

    def test_no_publish_when_only_skipped_signals(self, mock_cw):
        import observability
        health = {
            "signals": [
                {"signal": "elasticache_status", "skipped": True},
                {"signal": "api_gw_5xx",         "skipped": True},
            ],
        }
        observability.publish_signal_metrics(health, "us-east-1")
        # Empty MetricData → no put call.
        assert mock_cw.put_metric_data.call_count == 0

    def test_unknown_signal_name_is_ignored(self, mock_cw):
        import observability
        health = {
            "signals": [
                {"signal": "http_health",   "healthy": True},
                {"signal": "invented_thing", "healthy": False},
            ],
        }
        observability.publish_signal_metrics(health, "us-east-1")
        metric_data = _put_calls(mock_cw)[0]
        names = {m["MetricName"] for m in metric_data}
        assert names == {"SignalHttp"}

    def test_handles_missing_signals_list(self, mock_cw):
        import observability
        observability.publish_signal_metrics({}, "us-east-1")
        observability.publish_signal_metrics({"signals": None}, "us-east-1")
        observability.publish_signal_metrics(None, "us-east-1")
        assert mock_cw.put_metric_data.call_count == 0


# ---------------------------------------------------------------------------
# increment_counter
# ---------------------------------------------------------------------------

class TestIncrementCounter:
    def test_publishes_count_value_1(self, mock_cw):
        import observability
        observability.increment_counter("FailoversTriggered", "us-east-1")
        metric_data = _put_calls(mock_cw)[0]
        assert len(metric_data) == 1
        assert metric_data[0]["MetricName"] == "FailoversTriggered"
        assert metric_data[0]["Value"] == 1.0
        assert metric_data[0]["Unit"] == "Count"

    def test_extra_dimensions_appear_in_addition_to_common(self, mock_cw):
        import observability
        with patch.dict(os.environ, {"APP_NAME": "x", "ROUTING_MODE": "failover"}):
            observability.increment_counter(
                "PromotionsSucceeded", "us-east-1", dimensions={"Tier": "Aurora"},
            )
        dims = _all_dims(_put_calls(mock_cw)[0][0])
        assert dims == {
            "Region": "us-east-1",
            "AppName": "x",
            "RoutingMode": "failover",
            "Tier": "Aurora",
        }

    def test_swallows_exception(self, mock_cw):
        import observability
        mock_cw.put_metric_data.side_effect = RuntimeError("nope")
        observability.increment_counter("anything", "us-east-1")  # must not raise


# ---------------------------------------------------------------------------
# record_duration_seconds
# ---------------------------------------------------------------------------

class TestRecordDurationSeconds:
    def test_publishes_seconds_unit(self, mock_cw):
        import observability
        observability.record_duration_seconds(
            "AuroraPromotionDurationSeconds", 47.5, "us-east-1",
            dimensions={"Tier": "Aurora"},
        )
        metric_data = _put_calls(mock_cw)[0]
        assert metric_data[0]["MetricName"] == "AuroraPromotionDurationSeconds"
        assert metric_data[0]["Value"] == 47.5
        assert metric_data[0]["Unit"] == "Seconds"
        dims = _all_dims(metric_data[0])
        assert dims["Tier"] == "Aurora"
        assert dims["Region"] == "us-east-1"


# ---------------------------------------------------------------------------
# Namespace
# ---------------------------------------------------------------------------

class TestNamespace:
    def test_uses_cw_namespace_env_var(self, mock_cw):
        import observability
        with patch.dict(os.environ, {"CW_NAMESPACE": "Custom/MyApp"}):
            observability.publish_state_metrics({"latch_engaged": False}, "us-east-1")
        call = mock_cw.put_metric_data.call_args
        assert (call.kwargs.get("Namespace") or call[1].get("Namespace")) == "Custom/MyApp"

    def test_default_namespace_when_unset(self, mock_cw):
        import observability
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("CW_NAMESPACE", None)
            observability.publish_state_metrics({"latch_engaged": False}, "us-east-1")
        call = mock_cw.put_metric_data.call_args
        assert (call.kwargs.get("Namespace") or call[1].get("Namespace")) == "Custom/RegionFailover"
