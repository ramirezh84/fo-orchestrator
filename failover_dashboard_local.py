#!/usr/bin/env python3
"""
Failover Dashboard — Local Runner
====================================
Runs the failover dashboard on your local machine using Flask.
Assumes an IAM role to read failover state (DynamoDB or S3 backend).

Usage:
  pip install flask boto3
  python3 failover_dashboard_local.py

  Then open: http://localhost:5000

Configuration:
  Edit the variables below to match your environment.
"""

import json
import logging
from datetime import datetime, timezone
from flask import Flask, Response

import os

import boto3
from botocore.config import Config as BotoConfig

from state_backend import create_backend, S3StateBackend, DynamoDBStateBackend

# ===========================================================================
# CONFIGURATION — edit these to match your environment
# ===========================================================================
PRIMARY_REGION = "us-east-1"
SECONDARY_REGION = "us-east-2"
ACCOUNT_ALIAS = "Domestic Deposits"
REFRESH_SECONDS = 30

# IAM role to assume for state backend access.
# Leave empty to use your default CLI credentials.
ASSUME_ROLE_ARN = ""  # e.g., "arn:aws:iam::433607260168:role/your-read-role"

# State backend: "dynamodb" or "s3"
STATE_BACKEND = os.environ.get("STATE_BACKEND", "dynamodb")

# Map of app names to their resource names (DynamoDB table or S3 bucket)
FAILOVER_APPS = {
    "MCC": "app-failover-state",
    # "Spark": "spark-failover-state",
    # "Arsenal": "arsenal-failover-state",
}

# S3-specific: bucket name override (when STATE_BACKEND=s3, FAILOVER_APPS values are bucket names)
STATE_PREFIX = os.environ.get("STATE_PREFIX", "failover-state/")

# Flask settings
FLASK_HOST = "0.0.0.0"
FLASK_PORT = 5000
# ===========================================================================

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)

_client_config = BotoConfig(connect_timeout=5, read_timeout=10, retries={"max_attempts": 1})


def _get_session():
    """Get a boto3 session, optionally assuming a role."""
    if ASSUME_ROLE_ARN:
        logger.info(f"Assuming role: {ASSUME_ROLE_ARN}")
        sts = boto3.client("sts")
        creds = sts.assume_role(
            RoleArn=ASSUME_ROLE_ARN,
            RoleSessionName="failover-dashboard-local",
            DurationSeconds=3600,
        )["Credentials"]
        return boto3.Session(
            aws_access_key_id=creds["AccessKeyId"],
            aws_secret_access_key=creds["SecretAccessKey"],
            aws_session_token=creds["SessionToken"],
        )
    return boto3.Session()


def _read_app_state(session, app_name, resource_name):
    """Read failover state for one app. Tries primary region first, falls back to secondary."""
    for region in [PRIMARY_REGION, SECONDARY_REGION]:
        try:
            if STATE_BACKEND == "s3":
                backend = S3StateBackend(
                    bucket=resource_name, region=region, prefix=STATE_PREFIX,
                    client_config=_client_config,
                )
            else:
                backend = DynamoDBStateBackend(
                    table_name=resource_name, region=region, client_config=_client_config,
                )
            item = backend.get_state()
            if item:
                return {
                    "app": app_name,
                    "table": resource_name,
                    "read_from": region,
                    "active_region": item.get("active_region", "unknown"),
                    "state": item.get("state", "unknown"),
                    "latch_engaged": item.get("latch_engaged", False),
                    "consecutive_failures": int(item.get("consecutive_failures", 0)),
                    "aurora_pending": item.get("aurora_promotion_pending", False),
                    "initiated_by": item.get("initiated_by", ""),
                    "reason": item.get("reason", ""),
                    "last_failover_ts": item.get("last_failover_ts", ""),
                    "last_active_metric_ts": item.get("last_active_metric_ts", ""),
                    "error": None,
                }
        except Exception as e:
            logger.warning(f"Failed to read {resource_name} from {region}: {e}")
            continue

    return {
        "app": app_name,
        "table": resource_name,
        "error": "Could not read state from either region",
    }


def _status_color(state_data):
    """Return a status level for color coding."""
    if state_data.get("error"):
        return "error"
    state = state_data.get("state", "")
    failures = state_data.get("consecutive_failures", 0)
    latch = state_data.get("latch_engaged", False)
    aurora = state_data.get("aurora_pending", False)

    if state in ("FAILOVER_IN_PROGRESS", "FAILBACK_IN_PROGRESS"):
        return "critical"
    if aurora:
        return "critical"
    if state == "WAITING_AURORA_PROMOTION":
        return "critical"
    if latch:
        return "warning"
    if failures > 0:
        return "warning"
    if state in ("PRIMARY_ACTIVE", "SECONDARY_ACTIVE"):
        return "healthy"
    return "unknown"


def _build_html(states):
    """Build the complete HTML dashboard."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    title = ACCOUNT_ALIAS or "Failover Dashboard"

    healthy = sum(1 for s in states if _status_color(s) == "healthy")
    warning = sum(1 for s in states if _status_color(s) == "warning")
    critical = sum(1 for s in states if _status_color(s) in ("critical", "error"))
    total = len(states)

    rows = ""
    for s in states:
        color = _status_color(s)
        if s.get("error"):
            rows += f"""
            <tr class="status-{color}">
              <td class="app-name">{s['app']}</td>
              <td colspan="6" class="error-cell">{s['error']}</td>
            </tr>"""
            continue

        active = s["active_region"]
        active_short = "USE1" if "east-1" in active else "USE2"
        state = s["state"]
        failures = s["consecutive_failures"]

        last_ts = s.get("last_active_metric_ts", "")
        age = ""
        if last_ts:
            try:
                ts = datetime.fromisoformat(last_ts.replace("Z", "+00:00"))
                delta = (datetime.now(timezone.utc) - ts).total_seconds()
                if delta < 120:
                    age = f"{int(delta)}s ago"
                else:
                    age = f"{int(delta / 60)}m ago"
            except (ValueError, TypeError):
                age = "?"

        last_fo = s.get("last_failover_ts", "")
        fo_display = ""
        if last_fo and last_fo != "1970-01-01T00:00:00Z":
            try:
                fo_ts = datetime.fromisoformat(last_fo.replace("Z", "+00:00"))
                fo_delta = (datetime.now(timezone.utc) - fo_ts).total_seconds()
                if fo_delta < 3600:
                    fo_display = f"{int(fo_delta / 60)}m ago"
                elif fo_delta < 86400:
                    fo_display = f"{int(fo_delta / 3600)}h ago"
                else:
                    fo_display = f"{int(fo_delta / 86400)}d ago"
            except (ValueError, TypeError):
                fo_display = last_fo[:19]

        rows += f"""
            <tr class="status-{color}">
              <td class="app-name">{s['app']}</td>
              <td class="region-cell"><span class="region-badge region-{active_short.lower()}">{active_short}</span></td>
              <td class="state-cell">{state}</td>
              <td class="failures-cell">{'<span class="failure-count">' + str(failures) + '</span>' if failures > 0 else '0'}</td>
              <td class="latch-cell">{'<span class="latch-on">YES</span>' if s['latch_engaged'] else 'no'}</td>
              <td class="aurora-cell">{'<span class="aurora-pending">PENDING</span>' if s['aurora_pending'] else 'no'}</td>
              <td class="heartbeat-cell">{age}</td>
              <td class="last-fo-cell">{fo_display or 'never'}</td>
              <td class="reason-cell" title="{s.get('reason', '')}">{s.get('initiated_by', '')}</td>
            </tr>"""

    return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta http-equiv="refresh" content="{REFRESH_SECONDS}">
<title>Failover Dashboard - {title}</title>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{
    font-family: 'SF Mono', 'Fira Code', 'Cascadia Code', 'Consolas', monospace;
    background: #0a0e17;
    color: #c8d1dc;
    padding: 24px;
    min-height: 100vh;
  }}
  .header {{
    display: flex;
    justify-content: space-between;
    align-items: flex-end;
    margin-bottom: 32px;
    border-bottom: 1px solid #1e2a3a;
    padding-bottom: 16px;
  }}
  .header h1 {{
    font-size: 18px;
    font-weight: 600;
    color: #e8ecf1;
    letter-spacing: -0.5px;
  }}
  .header .subtitle {{
    font-size: 12px;
    color: #5a6a7e;
    margin-top: 4px;
  }}
  .header .timestamp {{
    font-size: 11px;
    color: #5a6a7e;
    text-align: right;
  }}
  .summary {{
    display: flex;
    gap: 16px;
    margin-bottom: 24px;
  }}
  .summary-card {{
    padding: 12px 20px;
    border-radius: 6px;
    border: 1px solid #1e2a3a;
    background: #0f1520;
  }}
  .summary-card .count {{
    font-size: 28px;
    font-weight: 700;
  }}
  .summary-card .label {{
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 1px;
    color: #5a6a7e;
    margin-top: 2px;
  }}
  .count-healthy {{ color: #34d399; }}
  .count-warning {{ color: #fbbf24; }}
  .count-critical {{ color: #f87171; }}
  .count-total {{ color: #818cf8; }}
  table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
  }}
  th {{
    text-align: left;
    padding: 10px 14px;
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 1.5px;
    color: #4a5568;
    border-bottom: 1px solid #1e2a3a;
    font-weight: 500;
  }}
  td {{
    padding: 12px 14px;
    border-bottom: 1px solid #111827;
  }}
  tr:hover {{ background: #111827; }}
  .app-name {{ font-weight: 600; color: #e8ecf1; }}
  .region-badge {{
    display: inline-block;
    padding: 2px 8px;
    border-radius: 3px;
    font-size: 11px;
    font-weight: 600;
  }}
  .region-use1 {{ background: #064e3b; color: #34d399; }}
  .region-use2 {{ background: #451a03; color: #fbbf24; }}
  .state-cell {{ color: #94a3b8; }}
  .failure-count {{ color: #fbbf24; font-weight: 700; }}
  .latch-on {{ color: #fbbf24; font-weight: 700; }}
  .aurora-pending {{ color: #f87171; font-weight: 700; animation: pulse 1.5s infinite; }}
  @keyframes pulse {{ 0%, 100% {{ opacity: 1; }} 50% {{ opacity: 0.4; }} }}
  .error-cell {{ color: #f87171; font-style: italic; }}
  .heartbeat-cell {{ color: #5a6a7e; }}
  .last-fo-cell {{ color: #5a6a7e; }}
  .reason-cell {{ color: #5a6a7e; max-width: 100px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}

  .status-healthy td:first-child {{ border-left: 3px solid #34d399; }}
  .status-warning td:first-child {{ border-left: 3px solid #fbbf24; }}
  .status-critical td:first-child {{ border-left: 3px solid #f87171; }}
  .status-error td:first-child {{ border-left: 3px solid #f87171; }}
  .status-unknown td:first-child {{ border-left: 3px solid #4a5568; }}

  .footer {{
    margin-top: 24px;
    font-size: 11px;
    color: #3a4558;
    display: flex;
    justify-content: space-between;
  }}
</style>
</head>
<body>
  <div class="header">
    <div>
      <h1>Failover Dashboard</h1>
      <div class="subtitle">{title} | {PRIMARY_REGION} / {SECONDARY_REGION}</div>
    </div>
    <div class="timestamp">Last refresh: {now}<br>Auto-refresh: {REFRESH_SECONDS}s</div>
  </div>

  <div class="summary">
    <div class="summary-card">
      <div class="count count-total">{total}</div>
      <div class="label">Total Apps</div>
    </div>
    <div class="summary-card">
      <div class="count count-healthy">{healthy}</div>
      <div class="label">Healthy</div>
    </div>
    <div class="summary-card">
      <div class="count count-warning">{warning}</div>
      <div class="label">Degraded</div>
    </div>
    <div class="summary-card">
      <div class="count count-critical">{critical}</div>
      <div class="label">Critical</div>
    </div>
  </div>

  <table>
    <thead>
      <tr>
        <th>Application</th>
        <th>Active</th>
        <th>State</th>
        <th>Failures</th>
        <th>Latch</th>
        <th>Aurora</th>
        <th>Heartbeat</th>
        <th>Last Failover</th>
        <th>Triggered By</th>
      </tr>
    </thead>
    <tbody>
      {rows}
    </tbody>
  </table>

  <div class="footer">
    <span>Failover Orchestrator Dashboard | Reads from configured state backend</span>
    <span>Local mode | http://localhost:{FLASK_PORT}</span>
  </div>
</body>
</html>"""


@app.route("/")
def dashboard():
    session = _get_session()
    states = []
    for app_name, table_name in sorted(FAILOVER_APPS.items()):
        states.append(_read_app_state(session, app_name, table_name))
    html = _build_html(states)
    return Response(html, mimetype="text/html")


@app.route("/api/state")
def api_state():
    """JSON endpoint for programmatic access."""
    session = _get_session()
    states = []
    for app_name, table_name in sorted(FAILOVER_APPS.items()):
        states.append(_read_app_state(session, app_name, table_name))
    return Response(
        json.dumps(states, indent=2, default=str),
        mimetype="application/json",
    )


if __name__ == "__main__":
    logger.info(f"Starting Failover Dashboard on http://localhost:{FLASK_PORT}")
    logger.info(f"Apps: {list(FAILOVER_APPS.keys())}")
    logger.info(f"Role: {ASSUME_ROLE_ARN or '(using default credentials)'}")
    logger.info(f"Regions: {PRIMARY_REGION} / {SECONDARY_REGION}")
    app.run(host=FLASK_HOST, port=FLASK_PORT, debug=True)
