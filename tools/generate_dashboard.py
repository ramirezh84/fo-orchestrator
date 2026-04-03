#!/usr/bin/env python3
"""
generate_dashboard.py — CloudWatch failover dashboard generator

Usage:
    # Generate JSON and print to stdout
    python3 generate_dashboard.py --config my-app.yaml

    # Write JSON to a file
    python3 generate_dashboard.py --config my-app.yaml --output my-app-dashboard.json

    # Deploy directly to CloudWatch
    python3 generate_dashboard.py --config my-app.yaml --deploy

    # Deploy to a specific region (default: primary_region from config)
    python3 generate_dashboard.py --config my-app.yaml --deploy --deploy-region us-east-1

See dashboard_config.example.yaml for the config file format and field descriptions.
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

REQUIRED_FIELDS = [
    "dashboard_name",
    "account_id",
    "app_name",
    "orchestrator_function",
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
    "primary_aurora_cluster",
    "secondary_aurora_cluster",
    "primary_alarm_name",
    "secondary_alarm_name",
    "primary_r53_hc_id",
    "secondary_r53_hc_id",
]


def load_config(path: str) -> dict:
    with open(path) as f:
        cfg = yaml.safe_load(f)

    errors = []
    for field in REQUIRED_FIELDS:
        val = cfg.get(field, "")
        if not val or "REPLACE" in str(val):
            errors.append(f"  - '{field}' is missing or still set to the placeholder value")

    if errors:
        print(f"[error] Config file '{path}' has unfilled values:")
        for e in errors:
            print(e)
        sys.exit(1)

    return cfg


# ---------------------------------------------------------------------------
# Dashboard builder
# ---------------------------------------------------------------------------

def build_dashboard(c: dict) -> dict:
    p   = c["primary_region"]
    s   = c["secondary_region"]
    app = c["app_name"]
    ns  = c["cw_namespace"]
    acct = c["account_id"]

    primary_alarm_arn   = f"arn:aws:cloudwatch:{p}:{acct}:alarm:{c['primary_alarm_name']}"
    secondary_alarm_arn = f"arn:aws:cloudwatch:{s}:{acct}:alarm:{c['secondary_alarm_name']}"

    widgets = []
    y = 0

    # ── Title ────────────────────────────────────────────────────────────────
    widgets.append(_text(
        f"# {app} — Failover Dashboard\n"
        f"Regions: **{p} (PRIMARY)** | **{s} (SECONDARY)**",
        x=0, y=y, w=24, h=1,
    ))
    y += 1

    # ── Alarm status ─────────────────────────────────────────────────────────
    widgets.append(_alarm(
        f"🔴 Orchestrator Alarm — {p} (PRIMARY)",
        [primary_alarm_arn], x=0, y=y, w=12, h=2,
    ))
    widgets.append(_alarm(
        f"🔵 Orchestrator Alarm — {s} (SECONDARY)",
        [secondary_alarm_arn], x=12, y=y, w=12, h=2,
    ))
    y += 2

    # ── RegionActiveStatus ───────────────────────────────────────────────────
    widgets.append(_metric(
        f"RegionActiveStatus — {p} (1=healthy, 0=failed)",
        region=p,
        metrics=[[ns, c["cw_metric"], "Region", p,
                  {"color": "#2ca02c", "label": f"{p} Active"}]],
        x=0, y=y, w=12, h=4,
        yaxis={"left": {"min": -0.1, "max": 1.5}},
        annotations={"horizontal": [
            {"value": 1, "color": "#2ca02c", "label": "Healthy"},
            {"value": 0, "color": "#d62728", "label": "Failed"},
        ]},
    ))
    widgets.append(_metric(
        f"RegionActiveStatus — {s} (1=healthy, 0=failed)",
        region=s,
        metrics=[[ns, c["cw_metric"], "Region", s,
                  {"color": "#1f77b4", "label": f"{s} Active"}]],
        x=12, y=y, w=12, h=4,
        yaxis={"left": {"min": -0.1, "max": 1.5}},
        annotations={"horizontal": [
            {"value": 1, "color": "#2ca02c", "label": "Healthy"},
            {"value": 0, "color": "#d62728", "label": "Failed"},
        ]},
    ))
    y += 4

    # ── ECS + ALB (primary row, then secondary row) ──────────────────────────
    for region, color, alb, tg in [
        (p, "#2ca02c", c["primary_alb_suffix"],   c["primary_tg_suffix"]),
        (s, "#1f77b4", c["secondary_alb_suffix"],  c["secondary_tg_suffix"]),
    ]:
        widgets.append(_metric(
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
            x=0, y=y, w=8, h=4,
            yaxis={"left": {"min": 0}},
        ))
        widgets.append(_metric(
            f"ALB Healthy Hosts — {region}",
            region=region,
            metrics=[
                ["AWS/ApplicationELB", "HealthyHostCount",
                 "TargetGroup", tg, "LoadBalancer", alb,
                 {"color": color, "label": "Healthy Hosts"}],
                ["AWS/ApplicationELB", "UnHealthyHostCount",
                 "TargetGroup", tg, "LoadBalancer", alb,
                 {"color": "#d62728", "label": "Unhealthy Hosts"}],
            ],
            x=8, y=y, w=8, h=4,
            yaxis={"left": {"min": 0}},
        ))
        widgets.append(_metric(
            f"ALB Request Count & 5xx — {region}",
            region=region,
            metrics=[
                ["AWS/ApplicationELB", "RequestCount", "LoadBalancer", alb,
                 {"color": color, "label": "Requests", "stat": "Sum"}],
                ["AWS/ApplicationELB", "HTTPCode_Target_5XX_Count", "LoadBalancer", alb,
                 {"color": "#d62728", "label": "5xx Errors", "stat": "Sum"}],
            ],
            x=16, y=y, w=8, h=4,
            yaxis={"left": {"min": 0}},
            stat="Sum",
        ))
        y += 4

    # ── Aurora ───────────────────────────────────────────────────────────────
    for region, color, cluster, xpos in [
        (p, "#2ca02c", c["primary_aurora_cluster"],   0),
        (s, "#1f77b4", c["secondary_aurora_cluster"], 12),
    ]:
        widgets.append(_metric(
            f"Aurora DB Connections — {region} ({cluster})",
            region=region,
            metrics=[
                ["AWS/RDS", "DatabaseConnections", "DBClusterIdentifier", cluster,
                 {"color": color, "label": "Connections"}],
                ["AWS/RDS", "CommitLatency", "DBClusterIdentifier", cluster,
                 {"color": "#ff7f0e", "label": "Commit Latency (ms)", "yAxis": "right"}],
            ],
            x=xpos, y=y, w=12, h=4,
            yaxis={"left": {"min": 0}, "right": {"min": 0}},
        ))
    y += 4

    # ── Orchestrator Lambda ───────────────────────────────────────────────────
    fn = c["orchestrator_function"]
    for region, color, xpos in [(p, "#2ca02c", 0), (s, "#1f77b4", 12)]:
        widgets.append(_metric(
            f"Orchestrator Lambda — {region}",
            region=region,
            metrics=[
                ["AWS/Lambda", "Invocations", "FunctionName", fn,
                 {"color": color, "label": "Invocations", "stat": "Sum"}],
                ["AWS/Lambda", "Errors", "FunctionName", fn,
                 {"color": "#d62728", "label": "Errors", "stat": "Sum"}],
                ["AWS/Lambda", "Duration", "FunctionName", fn,
                 {"color": "#ff7f0e", "label": "Duration (ms)", "yAxis": "right"}],
            ],
            x=xpos, y=y, w=12, h=4,
            yaxis={"left": {"min": 0}, "right": {"min": 0}},
            stat="Sum",
        ))
    y += 4

    # ── Route 53 health checks ────────────────────────────────────────────────
    for region, color, hc_id, xpos in [
        (p, "#2ca02c", c["primary_r53_hc_id"],   0),
        (s, "#1f77b4", c["secondary_r53_hc_id"], 12),
    ]:
        widgets.append(_metric(
            f"Route 53 Health Check — {region} (1=healthy)",
            region="us-east-1",   # Route 53 metrics always live in us-east-1
            metrics=[
                ["AWS/Route53", "HealthCheckStatus", "HealthCheckId", hc_id,
                 {"color": color, "label": "HC Status (1=healthy)"}],
                ["AWS/Route53", "HealthCheckPercentageHealthy", "HealthCheckId", hc_id,
                 {"color": "#aec7e8", "label": "% Healthy Checkers", "yAxis": "right"}],
            ],
            x=xpos, y=y, w=12, h=4,
            stat="Minimum",
            yaxis={"left": {"min": 0, "max": 1.2}, "right": {"min": 0, "max": 110}},
        ))
    y += 4

    return {"widgets": widgets}


# ---------------------------------------------------------------------------
# Widget helpers
# ---------------------------------------------------------------------------

def _text(markdown, x, y, w, h):
    return {"type": "text", "x": x, "y": y, "width": w, "height": h,
            "properties": {"markdown": markdown}}


def _alarm(title, alarm_arns, x, y, w, h):
    return {"type": "alarm", "x": x, "y": y, "width": w, "height": h,
            "properties": {"title": title, "alarms": alarm_arns}}


def _metric(title, region, metrics, x, y, w, h,
            period=60, stat="Average", yaxis=None, annotations=None):
    props = {
        "title": title,
        "view": "timeSeries",
        "stacked": False,
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
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Generate a CloudWatch failover dashboard from a YAML config file."
    )
    parser.add_argument(
        "--config", required=True, metavar="FILE",
        help="Path to the YAML config file (copy dashboard_config.example.yaml to start).",
    )
    parser.add_argument(
        "--output", metavar="FILE",
        help="Write dashboard JSON to this file (default: print to stdout).",
    )
    parser.add_argument(
        "--deploy", action="store_true",
        help="Deploy the dashboard to CloudWatch after generating it.",
    )
    parser.add_argument(
        "--deploy-region", metavar="REGION",
        help="AWS region for the dashboard (default: primary_region from config).",
    )
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

        deploy_region = args.deploy_region or cfg["primary_region"]
        dashboard_name = cfg["dashboard_name"]
        print(f"\nDeploying '{dashboard_name}' to {deploy_region}...", file=sys.stderr)
        boto3.client("cloudwatch", region_name=deploy_region).put_dashboard(
            DashboardName=dashboard_name,
            DashboardBody=json.dumps(body),
        )
        print(f"Done. Open at:", file=sys.stderr)
        print(f"  https://{deploy_region}.console.aws.amazon.com/cloudwatch/home"
              f"?region={deploy_region}#dashboards:name={dashboard_name}", file=sys.stderr)


if __name__ == "__main__":
    main()
