"""Observability metrics for the Vigil failover/failback Lambdas.

Provides four helpers used by both ``failover_orchestrator_v3.py`` and
``manual_failback_v2.py`` to publish CloudWatch metrics:

  * ``publish_state_metrics(state, region)`` — 4 state-machine metrics
    (LatchEngaged, AuroraPromotionPending, RedisPromotionPending,
    ConsecutiveFailures), batched into one ``put_metric_data`` call.

  * ``publish_signal_metrics(health, region)`` — per-signal 1/0 metrics
    (SignalHttp, SignalAlb, SignalEcs, SignalApiGw, SignalAurora,
    SignalElasticache). Signals with ``skipped: True`` are omitted, so
    deployments without ElastiCache (or API GW) don't pollute the metric
    stream with bogus zeroes.

  * ``increment_counter(metric_name, region, dimensions=None)`` —
    single-datapoint Count metric for lifecycle events (failovers,
    failbacks, promotion attempts/successes/failures).

  * ``record_duration_seconds(metric_name, seconds, region, dimensions=None)``
    — single-datapoint Seconds metric for promotion durations.

Every metric carries three dimensions read at call time from os.environ:

  * ``Region``   — caller-supplied (CURRENT_REGION in the orchestrator)
  * ``AppName``  — APP_NAME env var, defaulting to ``"(unset)"``
  * ``RoutingMode`` — ROUTING_MODE env var, defaulting to ``"failover"``

Plus any caller-supplied extra dimensions (e.g., ``Tier=Aurora|Redis`` on
promotion counters).

All four helpers swallow exceptions and log a warning — metrics must never
crash the handler. Both Lambda zips must include this file (see CLAUDE.md
deploy commands).
"""

import logging
import os
from typing import Optional

import boto3
from botocore.config import Config as BotoConfig

logger = logging.getLogger()

_cw_clients: dict = {}
_client_config = BotoConfig(
    connect_timeout=5, read_timeout=10, retries={"max_attempts": 1}
)

# CloudWatch signal name -> dashboard metric name. Source-of-truth signal
# names live in failover_orchestrator_v3.py's evaluate_*_health functions.
_SIGNAL_NAME_MAP = {
    "http_health":        "SignalHttp",
    "alb_healthy_hosts":  "SignalAlb",
    "ecs_running_tasks":  "SignalEcs",
    "api_gw_5xx":         "SignalApiGw",
    "aurora_status":      "SignalAurora",
    "elasticache_status": "SignalElasticache",
}


def _get_cw_client(region: str):
    if region not in _cw_clients:
        _cw_clients[region] = boto3.client(
            "cloudwatch", region_name=region, config=_client_config
        )
    return _cw_clients[region]


def _common_dimensions(region: str, extra: Optional[dict] = None) -> list:
    dims = [
        {"Name": "Region",      "Value": region},
        {"Name": "AppName",     "Value": os.environ.get("APP_NAME") or "(unset)"},
        {"Name": "RoutingMode", "Value": os.environ.get("ROUTING_MODE", "failover")},
    ]
    if extra:
        for k, v in extra.items():
            dims.append({"Name": str(k), "Value": str(v)})
    return dims


def _put(region: str, metric_data: list) -> None:
    if not metric_data:
        return
    namespace = os.environ.get("CW_NAMESPACE", "Custom/RegionFailover")
    try:
        _get_cw_client(region).put_metric_data(
            Namespace=namespace, MetricData=metric_data,
        )
    except Exception as e:
        logger.warning(
            f"observability: put_metric_data failed (non-fatal): "
            f"{type(e).__name__}: {e}"
        )


def publish_state_metrics(state: dict, region: str) -> None:
    """Publish the 4 state-machine metrics in one batched put."""
    if not isinstance(state, dict):
        return
    dims = _common_dimensions(region)
    metric_data = [
        {"MetricName": "LatchEngaged",
         "Dimensions": dims,
         "Value": 1.0 if state.get("latch_engaged") else 0.0,
         "Unit": "None"},
        {"MetricName": "AuroraPromotionPending",
         "Dimensions": dims,
         "Value": 1.0 if state.get("aurora_promotion_pending") else 0.0,
         "Unit": "None"},
        {"MetricName": "RedisPromotionPending",
         "Dimensions": dims,
         "Value": 1.0 if state.get("redis_promotion_pending") else 0.0,
         "Unit": "None"},
        {"MetricName": "ConsecutiveFailures",
         "Dimensions": dims,
         "Value": float(state.get("consecutive_failures") or 0),
         "Unit": "Count"},
    ]
    _put(region, metric_data)


def publish_signal_metrics(health: dict, region: str) -> None:
    """Publish one 1/0 metric per non-skipped health signal.

    Skipped signals (e.g., elasticache_status when Redis isn't configured)
    are omitted so the metric stream stays clean per deployment shape.
    """
    if not isinstance(health, dict):
        return
    signals = health.get("signals") or []
    dims = _common_dimensions(region)
    metric_data = []
    for s in signals:
        if not isinstance(s, dict) or s.get("skipped"):
            continue
        metric_name = _SIGNAL_NAME_MAP.get(s.get("signal"))
        if not metric_name:
            continue
        metric_data.append({
            "MetricName": metric_name,
            "Dimensions": dims,
            "Value": 1.0 if s.get("healthy") else 0.0,
            "Unit": "None",
        })
    _put(region, metric_data)


def increment_counter(
    metric_name: str, region: str, dimensions: Optional[dict] = None,
) -> None:
    """Publish +1 for a Count metric. Use for lifecycle events."""
    _put(region, [{
        "MetricName": metric_name,
        "Dimensions": _common_dimensions(region, dimensions),
        "Value": 1.0,
        "Unit": "Count",
    }])


def record_duration_seconds(
    metric_name: str, seconds: float, region: str,
    dimensions: Optional[dict] = None,
) -> None:
    """Publish a Seconds-unit datapoint. Use for promotion durations."""
    _put(region, [{
        "MetricName": metric_name,
        "Dimensions": _common_dimensions(region, dimensions),
        "Value": float(seconds),
        "Unit": "Seconds",
    }])
