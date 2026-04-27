#!/usr/bin/env python3
"""generate_dashboard.py — scenario-aware CloudWatch failover dashboard generator.

Reads a per-app YAML config and emits a CloudWatch dashboard tailored to the
app's deployment shape. Conditionally includes Aurora / ElastiCache / API GW
widget rows so the dashboard for an app-only stack doesn't show empty Aurora
graphs, and the dashboard for a no-Redis stack doesn't have a barren Redis row.

Consumes the metrics published by ``observability.py`` (issue #98):

  * Always-on:    LatchEngaged, AuroraPromotionPending, RedisPromotionPending,
                  ConsecutiveFailures, Signal* (per signal)
  * Event-driven: FailoversTriggered, FailbacksCompleted, Promotions*,
                  *PromotionDurationSeconds

Every metric carries ``Region`` + ``AppName`` + ``RoutingMode`` dimensions so a
multi-app dashboard could filter or group across them. This generator pins
``AppName=app_name`` and ``RoutingMode=routing_mode`` from the YAML so each app's
dashboard shows only that app's data.

Usage:
    python3 generate_dashboard.py --config my-app.yaml             # print JSON
    python3 generate_dashboard.py --config my-app.yaml --output o.json
    python3 generate_dashboard.py --config my-app.yaml --deploy   # put_dashboard
    python3 generate_dashboard.py --config my-app.yaml --deploy --deploy-region us-east-1
    python3 generate_dashboard.py --config my-app.yaml --deploy --profile tbed

See ``dashboard_config.example.yaml`` for the full schema.
"""

import argparse
import json
import sys

try:
    import yaml
except ImportError:
    sys.exit("PyYAML is required: pip install pyyaml")


# ---------------------------------------------------------------------------
# Config loading + validation
# ---------------------------------------------------------------------------

# Always required (every deployment shape needs these).
REQUIRED_FIELDS = [
    "dashboard_name",
    "account_id",
    "app_name",
    "orchestrator_function",
    "failback_function",
    "primary_region",
    "secondary_region",
    "cw_namespace",
    "cw_metric",
    "ecs_cluster",
    "ecs_service",
    "primary_alb_suffix",
    "secondary_alb_suffix",
    "primary_tg_suffix",
    "secondary_tg_suffix",
    "primary_alarm_name",
    "secondary_alarm_name",
    "primary_r53_hc_id",
    "secondary_r53_hc_id",
]

# Required only when the matching scenario flag is true.
CONDITIONAL_FIELDS = {
    "aurora_present":  ["primary_aurora_cluster", "secondary_aurora_cluster"],
    "redis_present":   ["primary_redis_rg", "secondary_redis_rg"],
    "api_gw_present":  ["api_gw_name"],
}

# Defaults applied when the flag is absent (back-compat with the pre-#100 schema
# which didn't have scenario flags). Default to active/passive, Aurora-only —
# matches the original example YAML's implicit shape.
SCENARIO_DEFAULTS = {
    "routing_mode":    "failover",
    "aurora_present":  True,
    "aurora_auto":     False,
    "redis_present":   False,
    "redis_auto":      False,
    "api_gw_present":  False,
}


def load_config(path: str) -> dict:
    with open(path) as f:
        cfg = yaml.safe_load(f) or {}

    for k, v in SCENARIO_DEFAULTS.items():
        cfg.setdefault(k, v)

    errors = []
    for field in REQUIRED_FIELDS:
        val = cfg.get(field, "")
        if not val or "REPLACE" in str(val):
            errors.append(f"  - '{field}' is missing or still the placeholder value")

    for flag, fields in CONDITIONAL_FIELDS.items():
        if cfg.get(flag):
            for field in fields:
                val = cfg.get(field, "")
                if not val or "REPLACE" in str(val):
                    errors.append(
                        f"  - '{field}' is required because '{flag}' is true, "
                        f"but it's missing/placeholder"
                    )

    if cfg.get("routing_mode") not in ("failover", "active-active"):
        errors.append(
            f"  - 'routing_mode' must be 'failover' or 'active-active', "
            f"got {cfg.get('routing_mode')!r}"
        )

    if errors:
        print(f"[error] Config file '{path}' has problems:")
        for e in errors:
            print(e)
        sys.exit(1)

    return cfg


# ---------------------------------------------------------------------------
# Widget primitives
# ---------------------------------------------------------------------------

def _text(markdown, x, y, w, h):
    return {"type": "text", "x": x, "y": y, "width": w, "height": h,
            "properties": {"markdown": markdown}}


def _alarm(title, alarm_arns, x, y, w, h):
    return {"type": "alarm", "x": x, "y": y, "width": w, "height": h,
            "properties": {"title": title, "alarms": alarm_arns}}


def _metric(title, region, metrics, x, y, w, h,
            period=60, stat="Average", yaxis=None, annotations=None,
            view="timeSeries", stacked=False):
    props = {
        "title": title,
        "view": view,
        "stacked": stacked,
        "region": region,
        "metrics": metrics,
        "period": period,
        "stat": stat,
    }
    if yaxis:
        props["yAxis"] = yaxis
    if annotations:
        props["annotations"] = annotations
    return {"type": "metric", "x": x, "y": y, "width": w, "height": h,
            "properties": props}


# ---------------------------------------------------------------------------
# Metric specs that consume observability.py output
# ---------------------------------------------------------------------------

def _vigil_metric(c: dict, name: str, region: str, color: str = None,
                  label: str = None, extra_dims: dict = None,
                  stat: str = None) -> list:
    """Build a metric spec for an observability.py-published metric.

    Auto-attaches the Region+AppName+RoutingMode key the helpers always emit,
    plus any ``extra_dims`` (e.g., ``Tier=Aurora``). Use this anywhere we want
    to graph one of the issue #98 metrics.
    """
    spec = [
        c["cw_namespace"], name,
        "Region",      region,
        "AppName",     c["app_name"],
        "RoutingMode", c["routing_mode"],
    ]
    if extra_dims:
        for k, v in extra_dims.items():
            spec += [str(k), str(v)]
    opts = {}
    if color:
        opts["color"] = color
    if label:
        opts["label"] = label
    if stat:
        opts["stat"] = stat
    if opts:
        spec.append(opts)
    return spec


# ---------------------------------------------------------------------------
# Row builders
# ---------------------------------------------------------------------------

def _scenario_tag(c: dict) -> str:
    aurora = "auto" if c["aurora_auto"] else ("manual" if c["aurora_present"] else "absent")
    redis = "auto" if c["redis_auto"] else ("manual" if c["redis_present"] else "absent")
    apigw = "present" if c["api_gw_present"] else "absent"
    return (
        f"[Scenario: {c['routing_mode']} · Aurora-{aurora} · "
        f"Redis-{redis} · APIgw-{apigw}]"
    )


def _row_title(c, y):
    p, s, app = c["primary_region"], c["secondary_region"], c["app_name"]
    return [_text(
        f"# {app} — Failover Dashboard\n"
        f"Regions: **{p} (PRIMARY)** | **{s} (SECONDARY)**\n\n"
        f"{_scenario_tag(c)}",
        x=0, y=y, w=24, h=2,
    )], 2


def _row_alarms(c, y):
    p, s, acct = c["primary_region"], c["secondary_region"], c["account_id"]
    p_arn = f"arn:aws:cloudwatch:{p}:{acct}:alarm:{c['primary_alarm_name']}"
    s_arn = f"arn:aws:cloudwatch:{s}:{acct}:alarm:{c['secondary_alarm_name']}"
    return [
        _alarm(f"Orchestrator Alarm — {p} (PRIMARY)",   [p_arn], x=0,  y=y, w=12, h=2),
        _alarm(f"Orchestrator Alarm — {s} (SECONDARY)", [s_arn], x=12, y=y, w=12, h=2),
    ], 2


def _row_region_active_status(c, y):
    """RegionActiveStatus per region, with LatchEngaged overlaid.

    Issue #102: previously labeled value=0 as 'Failed', which is misleading on
    a latched failed-over-from region — there RegionActiveStatus=0 is the v1.0
    anti-flip-flop latch's intended behavior, not a real failure. Overlaying
    LatchEngaged on the same widget lets an operator immediately distinguish
    'passive because latched' (LatchEngaged=1, RegionActiveStatus=0) from
    'unhealthy and lost the active role' (LatchEngaged=0, RegionActiveStatus=0).
    """
    p, s, ns, metric = c["primary_region"], c["secondary_region"], c["cw_namespace"], c["cw_metric"]
    yaxis = {"left": {"min": -0.1, "max": 1.5}}
    threshold_label = (
        "Healthy" if c["routing_mode"] == "active-active"
        else "Active region"
    )
    annotations = {"horizontal": [
        {"value": 1, "color": "#2ca02c", "label": threshold_label},
        {"value": 0, "color": "#7f7f7f", "label": "Inactive (passive or failed)"},
    ]}
    return [
        _metric(
            f"RegionActiveStatus — {p}",
            region=p,
            metrics=[
                [ns, metric, "Region", p,
                 {"color": "#2ca02c", "label": f"{p} active"}],
                _vigil_metric(c, "LatchEngaged", p,
                              color="#d62728", label="Latch engaged"),
            ],
            x=0, y=y, w=12, h=4, yaxis=yaxis, annotations=annotations,
        ),
        _metric(
            f"RegionActiveStatus — {s}",
            region=s,
            metrics=[
                [ns, metric, "Region", s,
                 {"color": "#1f77b4", "label": f"{s} active"}],
                _vigil_metric(c, "LatchEngaged", s,
                              color="#d62728", label="Latch engaged"),
            ],
            x=12, y=y, w=12, h=4, yaxis=yaxis, annotations=annotations,
        ),
    ], 4


def _row_per_signal(c, y):
    """Per-signal 1/0 metric panel. One sparkline per configured signal.

    Always includes HTTP, ALB, ECS. Conditionally adds API GW, Aurora,
    ElastiCache. Drawn for the primary region only — the per-signal data is
    the most operationally meaningful for the active region (passive region
    publishes its own signals via the same metric stream so a multi-region
    view can be built later if needed).
    """
    p = c["primary_region"]
    signals = [("SignalHttp", "HTTP",       "#2ca02c"),
               ("SignalAlb",  "ALB hosts",  "#1f77b4"),
               ("SignalEcs",  "ECS tasks",  "#ff7f0e")]
    if c["api_gw_present"]:
        signals.append(("SignalApiGw", "API GW", "#9467bd"))
    if c["aurora_present"]:
        signals.append(("SignalAurora", "Aurora", "#8c564b"))
    if c["redis_present"]:
        signals.append(("SignalElasticache", "Redis", "#e377c2"))

    metrics = [
        _vigil_metric(c, name, p, color=color, label=label)
        for name, label, color in signals
    ]
    return [_metric(
        f"Per-signal health — {p} (1=OK, 0=FAIL)",
        region=p,
        metrics=metrics,
        x=0, y=y, w=24, h=4,
        yaxis={"left": {"min": -0.1, "max": 1.5}},
        annotations={"horizontal": [{"value": 1, "color": "#2ca02c", "label": "OK"}]},
    )], 4


def _row_ecs_alb(c, y):
    """ECS RunningTasks + ALB HealthyHosts + 5xx — one row per region."""
    widgets = []
    for region, color, alb, tg in [
        (c["primary_region"],   "#2ca02c", c["primary_alb_suffix"],   c["primary_tg_suffix"]),
        (c["secondary_region"], "#1f77b4", c["secondary_alb_suffix"], c["secondary_tg_suffix"]),
    ]:
        widgets += [
            _metric(
                f"ECS Running Tasks — {region}",
                region=region,
                metrics=[
                    ["ECS/ContainerInsights", "RunningTaskCount",
                     "ServiceName", c["ecs_service"], "ClusterName", c["ecs_cluster"],
                     {"color": color, "label": "Running"}],
                    ["ECS/ContainerInsights", "DesiredTaskCount",
                     "ServiceName", c["ecs_service"], "ClusterName", c["ecs_cluster"],
                     {"color": "#aec7e8", "label": "Desired"}],
                ],
                x=0, y=y, w=8, h=4, yaxis={"left": {"min": 0}},
            ),
            _metric(
                f"ALB Healthy Hosts — {region}",
                region=region,
                metrics=[
                    ["AWS/ApplicationELB", "HealthyHostCount",
                     "TargetGroup", tg, "LoadBalancer", alb,
                     {"color": color, "label": "Healthy"}],
                    ["AWS/ApplicationELB", "UnHealthyHostCount",
                     "TargetGroup", tg, "LoadBalancer", alb,
                     {"color": "#d62728", "label": "Unhealthy"}],
                ],
                x=8, y=y, w=8, h=4, yaxis={"left": {"min": 0}},
            ),
            _metric(
                f"ALB Requests / 5xx — {region}",
                region=region,
                metrics=[
                    ["AWS/ApplicationELB", "RequestCount", "LoadBalancer", alb,
                     {"color": color, "label": "Requests", "stat": "Sum"}],
                    ["AWS/ApplicationELB", "HTTPCode_Target_5XX_Count", "LoadBalancer", alb,
                     {"color": "#d62728", "label": "5xx", "stat": "Sum"}],
                ],
                x=16, y=y, w=8, h=4, yaxis={"left": {"min": 0}}, stat="Sum",
            ),
        ]
        y += 4
    return widgets, 8  # 4 per region × 2 regions


def _row_aurora(c, y):
    widgets = []
    for region, color, cluster, xpos in [
        (c["primary_region"],   "#2ca02c", c["primary_aurora_cluster"],   0),
        (c["secondary_region"], "#1f77b4", c["secondary_aurora_cluster"], 12),
    ]:
        widgets.append(_metric(
            f"Aurora — {region} ({cluster})",
            region=region,
            metrics=[
                ["AWS/RDS", "DatabaseConnections", "DBClusterIdentifier", cluster,
                 {"color": color, "label": "Connections"}],
                ["AWS/RDS", "CommitLatency", "DBClusterIdentifier", cluster,
                 {"color": "#ff7f0e", "label": "Commit ms", "yAxis": "right"}],
                ["AWS/RDS", "AuroraGlobalDBReplicationLag", "DBClusterIdentifier", cluster,
                 {"color": "#9467bd", "label": "Repl lag ms", "yAxis": "right"}],
            ],
            x=xpos, y=y, w=12, h=4,
            yaxis={"left": {"min": 0}, "right": {"min": 0}},
        ))
    return widgets, 4


def _row_elasticache(c, y):
    widgets = []
    for region, color, rg, xpos in [
        (c["primary_region"],   "#2ca02c", c["primary_redis_rg"],   0),
        (c["secondary_region"], "#1f77b4", c["secondary_redis_rg"], 12),
    ]:
        widgets.append(_metric(
            f"ElastiCache — {region} ({rg})",
            region=region,
            metrics=[
                ["AWS/ElastiCache", "GlobalDatastoreReplicationLag", "ReplicationGroupId", rg,
                 {"color": color, "label": "Repl lag ms"}],
                ["AWS/ElastiCache", "EngineCPUUtilization", "ReplicationGroupId", rg,
                 {"color": "#ff7f0e", "label": "Engine CPU %", "yAxis": "right"}],
                ["AWS/ElastiCache", "DatabaseMemoryUsagePercentage", "ReplicationGroupId", rg,
                 {"color": "#9467bd", "label": "Memory %", "yAxis": "right"}],
            ],
            x=xpos, y=y, w=12, h=4,
            yaxis={"left": {"min": 0}, "right": {"min": 0, "max": 100}},
        ))
    return widgets, 4


def _row_api_gw(c, y):
    apigw = c["api_gw_name"]
    widgets = []
    for region, color, xpos in [
        (c["primary_region"],   "#2ca02c", 0),
        (c["secondary_region"], "#1f77b4", 12),
    ]:
        widgets.append(_metric(
            f"API Gateway — {region} ({apigw})",
            region=region,
            metrics=[
                ["AWS/ApiGateway", "Count",  "ApiName", apigw,
                 {"color": color, "label": "Total requests", "stat": "Sum"}],
                ["AWS/ApiGateway", "5XXError", "ApiName", apigw,
                 {"color": "#d62728", "label": "5xx", "stat": "Sum"}],
                ["AWS/ApiGateway", "Latency", "ApiName", apigw,
                 {"color": "#ff7f0e", "label": "Latency ms", "yAxis": "right"}],
            ],
            x=xpos, y=y, w=12, h=4, stat="Sum",
        ))
    return widgets, 4


def _row_lambda(c, y):
    widgets = []
    for fn_field, fn_label in [("orchestrator_function", "orchestrator"),
                                ("failback_function",     "failback")]:
        for region, color, xpos in [
            (c["primary_region"],   "#2ca02c", 0 if fn_field == "orchestrator_function" else 12),
            (c["secondary_region"], "#1f77b4", 0 if fn_field == "orchestrator_function" else 12),
        ]:
            pass  # placeholder — handled below
    # Cleaner: 4 widgets across 2 regions × 2 functions, but to keep visual
    # parity with the other rows we render orchestrator left, failback right
    # within each region row.
    widgets = []
    for fn_field, fn_label, xpos in [
        ("orchestrator_function", "orchestrator", 0),
        ("failback_function",     "failback",     12),
    ]:
        fn = c[fn_field]
        # Combine both regions on one widget per function so the chart density
        # stays manageable (was per-region in v1; that's 4 widgets vs the
        # dashboard's 24-col width).
        widgets.append(_metric(
            f"Lambda {fn_label} ({fn}) — both regions",
            region=c["primary_region"],
            metrics=[
                ["AWS/Lambda", "Invocations", "FunctionName", fn,
                 {"color": "#2ca02c", "label": f"{c['primary_region']} invocations",
                  "stat": "Sum", "region": c["primary_region"]}],
                ["AWS/Lambda", "Invocations", "FunctionName", fn,
                 {"color": "#1f77b4", "label": f"{c['secondary_region']} invocations",
                  "stat": "Sum", "region": c["secondary_region"]}],
                ["AWS/Lambda", "Errors", "FunctionName", fn,
                 {"color": "#d62728", "label": "Errors (both)", "stat": "Sum"}],
                ["AWS/Lambda", "Duration", "FunctionName", fn,
                 {"color": "#ff7f0e", "label": "Duration ms", "yAxis": "right"}],
            ],
            x=xpos, y=y, w=12, h=4, stat="Sum",
        ))
    return widgets, 4


def _row_route53(c, y):
    widgets = []
    for region, color, hc_id, xpos in [
        (c["primary_region"],   "#2ca02c", c["primary_r53_hc_id"],   0),
        (c["secondary_region"], "#1f77b4", c["secondary_r53_hc_id"], 12),
    ]:
        widgets.append(_metric(
            f"Route 53 Health Check — {region} (1=healthy)",
            # Route 53 metrics always live in us-east-1.
            region="us-east-1",
            metrics=[
                ["AWS/Route53", "HealthCheckStatus", "HealthCheckId", hc_id,
                 {"color": color, "label": "Status"}],
                ["AWS/Route53", "HealthCheckPercentageHealthy", "HealthCheckId", hc_id,
                 {"color": "#aec7e8", "label": "% checkers", "yAxis": "right"}],
            ],
            x=xpos, y=y, w=12, h=4, stat="Minimum",
            yaxis={"left": {"min": 0, "max": 1.2}, "right": {"min": 0, "max": 110}},
        ))
    return widgets, 4


def _row_state_machine(c, y):
    """State machine widgets consuming the issue #98 metrics."""
    p = c["primary_region"]
    threshold = c.get("consecutive_failures_threshold", 3)
    widgets = [
        _metric(
            "Latch + promotion pending — primary",
            region=p,
            metrics=[
                _vigil_metric(c, "LatchEngaged",          p, color="#d62728", label="Latch"),
                _vigil_metric(c, "AuroraPromotionPending", p, color="#ff7f0e", label="Aurora pending"),
                _vigil_metric(c, "RedisPromotionPending",  p, color="#9467bd", label="Redis pending"),
            ],
            x=0, y=y, w=12, h=4,
            yaxis={"left": {"min": -0.1, "max": 1.5}},
            annotations={"horizontal": [
                {"value": 1, "color": "#d62728", "label": "ENGAGED / pending"},
            ]},
        ),
        _metric(
            f"Consecutive failures — primary (threshold={threshold})",
            region=p,
            metrics=[
                _vigil_metric(c, "ConsecutiveFailures", p, color="#ff7f0e", label="Failures"),
            ],
            x=12, y=y, w=12, h=4,
            yaxis={"left": {"min": 0}},
            annotations={"horizontal": [
                {"value": threshold, "color": "#d62728", "label": f"Failover threshold ({threshold})"},
            ]},
        ),
    ]
    return widgets, 4


def _row_lifecycle_counters(c, y):
    """Cumulative event counters consuming the issue #98 metrics."""
    p = c["primary_region"]
    failover_metrics = [
        _vigil_metric(c, "FailoversTriggered",  p, color="#d62728",
                       label="Failovers", stat="Sum"),
        _vigil_metric(c, "FailbacksCompleted",  p, color="#2ca02c",
                       label="Failbacks", stat="Sum"),
    ]
    promotion_metrics = []
    if c["aurora_present"]:
        promotion_metrics += [
            _vigil_metric(c, "PromotionsAttempted", p, color="#1f77b4",
                           label="Aurora attempted",
                           extra_dims={"Tier": "Aurora"}, stat="Sum"),
            _vigil_metric(c, "PromotionsSucceeded", p, color="#2ca02c",
                           label="Aurora succeeded",
                           extra_dims={"Tier": "Aurora"}, stat="Sum"),
            _vigil_metric(c, "PromotionsFailed",    p, color="#d62728",
                           label="Aurora failed",
                           extra_dims={"Tier": "Aurora"}, stat="Sum"),
        ]
    if c["redis_present"]:
        promotion_metrics += [
            _vigil_metric(c, "PromotionsAttempted", p, color="#aec7e8",
                           label="Redis attempted",
                           extra_dims={"Tier": "Redis"}, stat="Sum"),
            _vigil_metric(c, "PromotionsSucceeded", p, color="#98df8a",
                           label="Redis succeeded",
                           extra_dims={"Tier": "Redis"}, stat="Sum"),
            _vigil_metric(c, "PromotionsFailed",    p, color="#ff9896",
                           label="Redis failed",
                           extra_dims={"Tier": "Redis"}, stat="Sum"),
        ]
    widgets = [
        _metric(
            "Lifecycle counters — failover/failback (Sum over period)",
            region=p, metrics=failover_metrics,
            x=0, y=y, w=12, h=4, stat="Sum",
        ),
    ]
    if promotion_metrics:
        widgets.append(_metric(
            "Promotion counters — attempted / succeeded / failed (Sum)",
            region=p, metrics=promotion_metrics,
            x=12, y=y, w=12, h=4, stat="Sum",
        ))
    else:
        widgets.append(_text(
            "No data tier configured (app-only stack) — promotion counters omitted.",
            x=12, y=y, w=12, h=4,
        ))
    return widgets, 4


def _row_promotion_durations(c, y):
    """AuroraPromotionDurationSeconds + RedisPromotionDurationSeconds.

    Only rendered when at least one tier is present. Returns (widgets, height).
    """
    if not (c["aurora_present"] or c["redis_present"]):
        return [], 0
    p = c["primary_region"]
    metrics = []
    tier_names = []
    if c["aurora_present"]:
        metrics.append(_vigil_metric(
            c, "AuroraPromotionDurationSeconds", p,
            color="#1f77b4", label="Aurora",
            extra_dims={"Tier": "Aurora"},
        ))
        tier_names.append("Aurora")
    if c["redis_present"]:
        metrics.append(_vigil_metric(
            c, "RedisPromotionDurationSeconds", p,
            color="#9467bd", label="Redis",
            extra_dims={"Tier": "Redis"},
        ))
        tier_names.append("Redis")
    title = f"Promotion durations — {' and '.join(tier_names)} (seconds)"
    return [_metric(
        title,
        region=p, metrics=metrics,
        x=0, y=y, w=24, h=4,
        yaxis={"left": {"min": 0}}, stat="Maximum",
    )], 4


# ---------------------------------------------------------------------------
# Dashboard assembly
# ---------------------------------------------------------------------------

def build_dashboard(c: dict) -> dict:
    """Compose the full dashboard from conditional rows."""
    widgets: list = []
    y = 0

    builders = [
        _row_title,
        _row_alarms,
        _row_region_active_status,
        _row_per_signal,
        _row_ecs_alb,
    ]
    if c["aurora_present"]:
        builders.append(_row_aurora)
    if c["redis_present"]:
        builders.append(_row_elasticache)
    if c["api_gw_present"]:
        builders.append(_row_api_gw)
    builders += [
        _row_lambda,
        _row_route53,
        _row_state_machine,
        _row_lifecycle_counters,
        _row_promotion_durations,
    ]

    for build in builders:
        row_widgets, h = build(c, y)
        widgets.extend(row_widgets)
        y += h

    return {"widgets": widgets}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Generate a scenario-aware CloudWatch failover dashboard from YAML.",
    )
    parser.add_argument("--config", required=True, metavar="FILE",
                        help="Path to YAML config (copy dashboard_config.example.yaml).")
    parser.add_argument("--output", metavar="FILE",
                        help="Write JSON to a file (default: stdout).")
    parser.add_argument("--deploy", action="store_true",
                        help="Deploy the dashboard via cloudwatch:PutDashboard.")
    parser.add_argument("--deploy-region", metavar="REGION",
                        help="Region for the dashboard (default: primary_region).")
    parser.add_argument("--profile", metavar="NAME",
                        help="AWS profile for --deploy.")
    args = parser.parse_args()

    cfg  = load_config(args.config)
    body = build_dashboard(cfg)
    body_str = json.dumps(body, indent=2)

    if args.output:
        with open(args.output, "w") as f:
            f.write(body_str)
        print(f"Dashboard JSON written to {args.output}")
    else:
        print(body_str)

    if args.deploy:
        try:
            import boto3
        except ImportError:
            sys.exit("boto3 is required for --deploy: pip install boto3")
        session = (boto3.Session(profile_name=args.profile)
                   if args.profile else boto3.Session())
        deploy_region = args.deploy_region or cfg["primary_region"]
        dashboard_name = cfg["dashboard_name"]
        print(f"\nDeploying '{dashboard_name}' to {deploy_region}...", file=sys.stderr)
        session.client("cloudwatch", region_name=deploy_region).put_dashboard(
            DashboardName=dashboard_name,
            DashboardBody=json.dumps(body),
        )
        print("Done. Open at:", file=sys.stderr)
        print(f"  https://{deploy_region}.console.aws.amazon.com/cloudwatch/home"
              f"?region={deploy_region}#dashboards:name={dashboard_name}",
              file=sys.stderr)


if __name__ == "__main__":
    main()
