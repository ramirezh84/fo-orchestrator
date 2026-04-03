#!/usr/bin/env python3
"""
Failover Orchestrator — Operator CLI with Live Monitor
========================================================
Interactive menu with a live health check monitor that continuously
pings the application URL and shows response status + timing.

Includes failure simulation via target group de-registration.

Usage:
  python3 failover_cli.py

Requirements:
  - Python 3.7+
  - boto3 installed (pip install boto3)
  - AWS CLI configured with valid credentials

Demo Flow:
  See option 'h' in the menu for step-by-step instructions.
"""

import json
import sys
import time
import threading
import os
import socket
from datetime import datetime
from urllib.parse import urlparse
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
import ssl

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    print("ERROR: boto3 is required. Install with: pip install boto3")
    sys.exit(1)



# ===========================================================================
# CONFIGURATION — edit these to match your environment
# ===========================================================================
PRIMARY_REGION = "us-west-1"
SECONDARY_REGION = "us-west-2"

ORCHESTRATOR_FUNCTION_NAME = "fo-demo-orchestrator"
FAILBACK_FUNCTION_NAME = "fo-demo-failback"
STATE_TABLE = "fo-demo-state"
STATE_TABLE_KEY = "REGION_STATE"

# State backend: "dynamodb" or "s3"
# When "s3", set STATE_BUCKET to the primary region's bucket name.
STATE_BACKEND_TYPE = "dynamodb"
STATE_BUCKET = ""  # e.g., "fo-demo-state-us-west-1-597088043823"
STATE_PREFIX = "failover-state/"

# The user-facing URL to monitor during failover
# NOTE: NLB requires Host header matching API GW domain — set below
MONITOR_URL = "https://api.testpoc.name/healthcheck"
MONITOR_HOST_HEADER = ""  # No override needed — custom domain routes correctly
MONITOR_INTERVAL_SECONDS = 2

# SSL verification (set False for self-signed certs)
MONITOR_SSL_VERIFY = False

# Target group for failure simulation (de-register targets to simulate failure)
# This is the TG name — the script looks up the full ARN automatically.
FAILURE_SIM_TARGET_GROUP_NAME = "fo-demo-fargate-tg"
FAILURE_SIM_REGION = "us-west-1"
FAILURE_SIM_INTERVAL_SECONDS = 5  # How often to check for and kill new targets

# NLB hostnames for region detection. The monitor resolves these at startup
# to build IP-to-region mappings, then on each ping resolves the app hostname
# and matches IPs to determine which region is serving traffic.
NLB_PRIMARY_HOSTNAME = "fo-demo-ext-nlb-ae5c75220449acb6.elb.us-west-1.amazonaws.com"
NLB_SECONDARY_HOSTNAME = "fo-demo-ext-nlb-6da401a283025e9c.elb.us-west-2.amazonaws.com"
# ===========================================================================

# Globals for the monitor thread
_monitor_running = False
_monitor_thread = None
_monitor_paused = False
_monitor_stats = {
    "total": 0,
    "success": 0,
    "fail": 0,
    "first_fail_time": None,
    "recovery_time": None,
    "was_failing": False,
    "downtime_seconds": 0,
}

# Globals for failure simulation thread
_failure_sim_running = False
_failure_sim_thread = None
_targets_deregistered = 0

# Globals for region detection (populated at startup)
_primary_ips = set()
_secondary_ips = set()
_last_detected_region = None

# Colors
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"


def clear_screen():
    os.system("cls" if os.name == "nt" else "clear")


def get_lambda_client(region):
    return boto3.client("lambda", region_name=region)


def get_state_backend(region):
    """Create a state backend for the given region using CLI config."""
    if STATE_BACKEND_TYPE == "s3":
        from state_backend import S3StateBackend
        return S3StateBackend(bucket=STATE_BUCKET, region=region, prefix=STATE_PREFIX)
    else:
        from state_backend import DynamoDBStateBackend
        return DynamoDBStateBackend(table_name=STATE_TABLE, region=region)


# ===========================================================================
# DNS Region Detection
# ===========================================================================

def resolve_hostname(hostname):
    """Resolve a hostname to a set of IPv4 addresses."""
    try:
        results = socket.getaddrinfo(hostname, 443, socket.AF_INET, socket.SOCK_STREAM)
        return set(r[4][0] for r in results)
    except socket.gaierror:
        return set()


def init_region_ips():
    """Resolve NLB hostnames at startup to build IP-to-region mappings."""
    global _primary_ips, _secondary_ips

    print(f"  {DIM}Resolving NLB IPs for region detection...{RESET}")

    _primary_ips = resolve_hostname(NLB_PRIMARY_HOSTNAME)
    _secondary_ips = resolve_hostname(NLB_SECONDARY_HOSTNAME)

    if _primary_ips:
        print(f"  {DIM}  {PRIMARY_REGION}: {', '.join(sorted(_primary_ips))}{RESET}")
    else:
        print(f"  {YELLOW}  {PRIMARY_REGION}: Could not resolve {NLB_PRIMARY_HOSTNAME}{RESET}")

    if _secondary_ips:
        print(f"  {DIM}  {SECONDARY_REGION}: {', '.join(sorted(_secondary_ips))}{RESET}")
    else:
        print(f"  {YELLOW}  {SECONDARY_REGION}: Could not resolve {NLB_SECONDARY_HOSTNAME}{RESET}")

    if not _primary_ips and not _secondary_ips:
        print(f"  {YELLOW}  Region detection disabled (no NLB IPs resolved){RESET}")

    print()


def detect_region():
    """Resolve the app hostname and match IPs to determine serving region."""
    if not _primary_ips and not _secondary_ips:
        return None

    hostname = urlparse(MONITOR_URL).hostname
    current_ips = resolve_hostname(hostname)

    if not current_ips:
        return None

    primary_match = len(current_ips & _primary_ips)
    secondary_match = len(current_ips & _secondary_ips)

    if primary_match > 0 and secondary_match == 0:
        return PRIMARY_REGION
    elif secondary_match > 0 and primary_match == 0:
        return SECONDARY_REGION
    elif primary_match > 0 and secondary_match > 0:
        return "BOTH"  # Transitional state during DNS change
    else:
        return "UNKNOWN"


def check_url(url):
    """Check URL and return (status, response_time_ms, details)."""
    ctx = None
    if not MONITOR_SSL_VERIFY:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

    start = time.time()
    try:
        req = Request(url, method="GET")
        req.add_header("User-Agent", "FailoverCLI/1.0")
        req.add_header("Accept", "application/json")
        if MONITOR_HOST_HEADER:
            req.add_header("Host", MONITOR_HOST_HEADER)
        with urlopen(req, timeout=5, context=ctx) as response:
            elapsed = (time.time() - start) * 1000
            status_code = response.getcode()
            body = response.read().decode("utf-8", errors="replace")

            status = "UNKNOWN"
            try:
                parsed = json.loads(body)
                status = parsed.get("status", "UNKNOWN")
            except (json.JSONDecodeError, ValueError):
                pass

            if status_code == 200:
                return ("UP", elapsed, f"HTTP {status_code} | {status}")
            else:
                return ("DOWN", elapsed, f"HTTP {status_code} | {status}")

    except HTTPError as e:
        elapsed = (time.time() - start) * 1000
        return ("DOWN", elapsed, f"HTTP {e.code}: {e.reason}")
    except URLError as e:
        elapsed = (time.time() - start) * 1000
        return ("DOWN", elapsed, f"Connection failed: {e.reason}")
    except Exception as e:
        elapsed = (time.time() - start) * 1000
        return ("DOWN", elapsed, f"{type(e).__name__}: {e}")


# ===========================================================================
# Health Monitor
# ===========================================================================

def monitor_loop():
    """Background thread that continuously monitors the URL."""
    global _monitor_running, _monitor_stats, _last_detected_region

    while _monitor_running:
        if _monitor_paused:
            time.sleep(0.5)
            continue

        now = datetime.now()
        timestamp = now.strftime("%H:%M:%S")

        # Detect which region is serving traffic
        current_region = detect_region()

        # Detect region switch
        if (current_region and _last_detected_region
                and current_region != _last_detected_region
                and current_region not in ("UNKNOWN", "BOTH")
                and _last_detected_region not in ("UNKNOWN", "BOTH")):
            print()
            print(
                f"  {BOLD}{CYAN}>>> REGION SWITCH: "
                f"{_last_detected_region} -> {current_region}{RESET} "
                f"at {timestamp}"
            )
            print()
        _last_detected_region = current_region

        # Format region tag
        if current_region == PRIMARY_REGION:
            region_tag = f"{GREEN}{PRIMARY_REGION}{RESET}"
        elif current_region == SECONDARY_REGION:
            region_tag = f"{YELLOW}{SECONDARY_REGION}{RESET}"
        elif current_region == "BOTH":
            region_tag = f"{CYAN}SWITCHING{RESET}"
        else:
            region_tag = f"{DIM}???{RESET}"

        status, latency, details = check_url(MONITOR_URL)
        _monitor_stats["total"] += 1

        if status == "UP":
            _monitor_stats["success"] += 1
            color = GREEN
            icon = "+"

            if _monitor_stats["was_failing"] and _monitor_stats["first_fail_time"]:
                _monitor_stats["recovery_time"] = now
                downtime = (now - _monitor_stats["first_fail_time"]).total_seconds()
                _monitor_stats["downtime_seconds"] = downtime
                _monitor_stats["was_failing"] = False
                print(
                    f"  {BOLD}{GREEN}>>> RECOVERED{RESET} at {timestamp} | "
                    f"Downtime: {BOLD}{downtime:.1f}s{RESET} | "
                    f"Now serving from: {BOLD}{current_region or '???'}{RESET}"
                )
                print()
        else:
            _monitor_stats["fail"] += 1
            color = RED
            icon = "X"

            if not _monitor_stats["was_failing"]:
                _monitor_stats["first_fail_time"] = now
                _monitor_stats["was_failing"] = True
                _monitor_stats["recovery_time"] = None
                print()
                print(
                    f"  {BOLD}{RED}>>> FAILURE DETECTED{RESET} at {timestamp}"
                )

        # Failure sim indicator
        sim_tag = f"  {RED}[SIM]{RESET}" if _failure_sim_running else ""

        latency_color = GREEN if latency < 200 else (YELLOW if latency < 1000 else RED)
        print(
            f"  {DIM}{timestamp}{RESET}  "
            f"{color}{icon}{RESET}  "
            f"{color}{status:4s}{RESET}  "
            f"{latency_color}{latency:7.0f}ms{RESET}  "
            f"[{region_tag}]  "
            f"{DIM}{details}{RESET}"
            f"{sim_tag}"
        )

        time.sleep(MONITOR_INTERVAL_SECONDS)


def start_monitor():
    global _monitor_running, _monitor_thread, _monitor_stats

    _monitor_stats = {
        "total": 0,
        "success": 0,
        "fail": 0,
        "first_fail_time": None,
        "recovery_time": None,
        "was_failing": False,
        "downtime_seconds": 0,
    }

    _monitor_running = True
    _monitor_thread = threading.Thread(target=monitor_loop, daemon=True)
    _monitor_thread.start()


def stop_monitor():
    global _monitor_running
    _monitor_running = False
    if _monitor_thread:
        _monitor_thread.join(timeout=3)


def pause_monitor():
    global _monitor_paused
    _monitor_paused = True


def resume_monitor():
    global _monitor_paused
    _monitor_paused = False


# ===========================================================================
# Failure Simulation (Target Group De-registration)
# ===========================================================================

def get_target_group_arn():
    """Look up the full TG ARN from the name."""
    try:
        elbv2 = boto3.client("elbv2", region_name=FAILURE_SIM_REGION)
        response = elbv2.describe_target_groups(
            Names=[FAILURE_SIM_TARGET_GROUP_NAME]
        )
        tgs = response.get("TargetGroups", [])
        if tgs:
            return tgs[0]["TargetGroupArn"]
        return None
    except Exception as e:
        print(f"  {RED}Cannot find target group: {e}{RESET}")
        return None


def deregister_all_targets(tg_arn):
    """Find all registered targets and de-register them. Returns count."""
    elbv2 = boto3.client("elbv2", region_name=FAILURE_SIM_REGION)
    try:
        response = elbv2.describe_target_health(TargetGroupArn=tg_arn)
        targets = response.get("TargetHealthDescriptions", [])

        active_targets = [
            t for t in targets
            if t.get("TargetHealth", {}).get("State") != "draining"
        ]

        if not active_targets:
            return 0

        target_ids = [
            {"Id": t["Target"]["Id"], "Port": t["Target"]["Port"]}
            for t in active_targets
        ]

        elbv2.deregister_targets(
            TargetGroupArn=tg_arn,
            Targets=target_ids,
        )

        return len(target_ids)

    except Exception as e:
        print(f"  {RED}De-register error: {e}{RESET}")
        return 0


def failure_sim_loop(tg_arn):
    """Background thread that continuously de-registers targets."""
    global _failure_sim_running, _targets_deregistered

    while _failure_sim_running:
        count = deregister_all_targets(tg_arn)
        if count > 0:
            _targets_deregistered += count
            now = datetime.now().strftime("%H:%M:%S")
            print(
                f"  {RED}[SIM] {now} De-registered {count} target(s) "
                f"(total: {_targets_deregistered}){RESET}"
            )

        time.sleep(FAILURE_SIM_INTERVAL_SECONDS)


def start_failure_sim():
    """Start the failure simulation."""
    global _failure_sim_running, _failure_sim_thread, _targets_deregistered

    print(f"\n  {BOLD}{RED}START FAILURE SIMULATION{RESET}")
    print(f"  {'-' * 50}")
    print(f"  Target group: {FAILURE_SIM_TARGET_GROUP_NAME}")
    print(f"  Region: {FAILURE_SIM_REGION}")
    print(f"  Interval: every {FAILURE_SIM_INTERVAL_SECONDS}s")
    print()
    print(f"  This will continuously de-register all targets from the")
    print(f"  target group, including new ones that ECS registers.")
    print(f"  The app will become unreachable, triggering the orchestrator.")

    confirm = input(
        f"\n  {BOLD}Start failure simulation?{RESET} (yes/no): "
    ).strip().lower()
    if confirm != "yes":
        print("  Cancelled.")
        return

    print(f"\n  {CYAN}Looking up target group ARN...{RESET}")
    tg_arn = get_target_group_arn()
    if not tg_arn:
        print(f"  {RED}Cannot find target group. Check the name.{RESET}")
        return

    print(f"  {DIM}ARN: {tg_arn}{RESET}")

    # Initial de-registration
    count = deregister_all_targets(tg_arn)
    _targets_deregistered = count
    print(f"  {RED}De-registered {count} target(s){RESET}")

    # Start background loop
    _failure_sim_running = True
    _failure_sim_thread = threading.Thread(
        target=failure_sim_loop, args=(tg_arn,), daemon=True
    )
    _failure_sim_thread.start()

    print(f"\n  {BOLD}{RED}Failure simulation ACTIVE{RESET}")
    print(f"  New targets will be de-registered every "
          f"{FAILURE_SIM_INTERVAL_SECONDS}s.")
    print(f"  Use option 7 to stop the simulation.")


def stop_failure_sim():
    """Stop the failure simulation."""
    global _failure_sim_running

    if not _failure_sim_running:
        print(f"\n  {YELLOW}Failure simulation is not running.{RESET}")
        return

    _failure_sim_running = False
    if _failure_sim_thread:
        _failure_sim_thread.join(timeout=10)

    print(f"\n  {GREEN}Failure simulation STOPPED{RESET}")
    print(f"  Total targets de-registered: {_targets_deregistered}")
    print(f"  ECS will re-register new tasks automatically.")
    print(f"  The app should recover within 1-2 minutes.")


# ===========================================================================
# Lambda Operations
# ===========================================================================

def invoke_lambda(function_name, region, payload):
    """Invoke a Lambda function and return the response."""
    client = get_lambda_client(region)
    print(f"\n  {CYAN}Invoking {function_name} in {region}...{RESET}")
    print(f"  {DIM}Payload: {json.dumps(payload)}{RESET}")

    try:
        response = client.invoke(
            FunctionName=function_name,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload),
        )
        status_code = response["StatusCode"]
        response_payload = json.loads(
            response["Payload"].read().decode("utf-8")
        )

        if "FunctionError" in response:
            print(f"\n  {RED}LAMBDA ERROR (status {status_code}):{RESET}")
            print(f"  {json.dumps(response_payload, indent=2)}")
            return None

        print(f"\n  {GREEN}Response (status {status_code}):{RESET}")
        if isinstance(response_payload, dict) and "body" in response_payload:
            body = response_payload["body"]
            for line in str(body).split("\n"):
                if line.strip():
                    print(f"  {line}")
        else:
            print(f"  {json.dumps(response_payload, indent=2)}")

        return response_payload

    except ClientError as e:
        print(f"  {RED}ERROR: {e}{RESET}")
        return None
    except Exception as e:
        print(f"  {RED}ERROR: {type(e).__name__}: {e}{RESET}")
        return None


def show_state():
    """Read and display current failover state."""
    print(f"\n  {BOLD}Current Failover State:{RESET}")
    print(f"  {'-' * 50}")

    try:
        backend = get_state_backend(PRIMARY_REGION)
        item = backend.get_state()

        if not item:
            print(f"  {YELLOW}No state found (table may be empty){RESET}")
            return

        active = item.get("active_region", "N/A")
        state = item.get("state", "N/A")
        latch = item.get("latch_engaged", "N/A")
        failures = item.get("consecutive_failures", "N/A")
        aurora = item.get("aurora_promotion_pending", "N/A")

        active_color = GREEN if active == PRIMARY_REGION else YELLOW
        state_color = GREEN if state == "PRIMARY_ACTIVE" else (
            YELLOW if state == "SECONDARY_ACTIVE" else RED
        )
        latch_color = RED if latch else GREEN

        print(f"  Active Region:        {active_color}{active}{RESET}")
        print(f"  State:                {state_color}{state}{RESET}")
        print(f"  Latch Engaged:        {latch_color}{latch}{RESET}")
        print(f"  Consecutive Failures: {failures}")
        print(f"  Aurora Pending:       {aurora}")
        print(f"  Last Failover:        {item.get('last_failover_ts', 'N/A')}")
        print(f"  Reason:               {item.get('reason', 'N/A')}")

    except Exception as e:
        print(f"  {RED}ERROR reading state: {e}{RESET}")


def do_failover():
    """Execute failover from the active region."""
    print(f"\n  {BOLD}{RED}EXECUTE FAILOVER{RESET}")
    print(f"  {'-' * 50}")
    print(f"  This will move DNS from the active region to standby.")
    print(f"  Aurora promotion will need to be done manually.")

    show_state()

    region = input(
        f"\n  Execute from which region? [{PRIMARY_REGION}]: "
    ).strip()
    if not region:
        region = PRIMARY_REGION

    confirm = input(
        f"  {BOLD}Confirm failover from {region}?{RESET} (yes/no): "
    ).strip().lower()
    if confirm != "yes":
        print("  Cancelled.")
        return

    result = invoke_lambda(
        ORCHESTRATOR_FUNCTION_NAME,
        region,
        {"execute_failover": True},
    )

    if result:
        print(f"\n  {BOLD}{GREEN}Failover executed. Watch the monitor "
              f"for traffic recovery.{RESET}")


def do_failback():
    """Execute failback to a target region."""
    print(f"\n  {BOLD}{CYAN}EXECUTE FAILBACK{RESET}")
    print(f"  {'-' * 50}")

    show_state()

    print(f"\n  Target regions:")
    print(f"    1. {PRIMARY_REGION}")
    print(f"    2. {SECONDARY_REGION}")
    choice = input(f"  Fail back to? [1]: ").strip()
    target = SECONDARY_REGION if choice == "2" else PRIMARY_REGION

    operator = input(f"  Your name: ").strip() or "operator"

    print(f"\n  Validation:")
    print(f"    1. Full (HTTP + ECS + Aurora)")
    print(f"    2. Skip health checks")
    skip = input(f"  Mode? [1]: ").strip() == "2"

    aurora = input(
        f"\n  Aurora already switched to {target}? (yes/no): "
    ).strip().lower()
    aurora_confirmed = aurora == "yes"

    if not aurora_confirmed:
        print(f"\n  {YELLOW}The Lambda will return Aurora switchover "
              f"commands.{RESET}")

    confirm = input(
        f"\n  {BOLD}Confirm failback to {target}?{RESET} (yes/no): "
    ).strip().lower()
    if confirm != "yes":
        print("  Cancelled.")
        return

    result = invoke_lambda(
        FAILBACK_FUNCTION_NAME,
        target,
        {
            "target_region": target,
            "skip_health_check": skip,
            "operator": operator,
            "aurora_confirmed": aurora_confirmed,
        },
    )

    if result:
        print(f"\n  {BOLD}{GREEN}Failback complete. Watch the monitor.{RESET}")


def do_reset():
    """Reset state to PRIMARY_ACTIVE."""
    print(f"\n  {BOLD}{YELLOW}RESET STATE{RESET}")
    print(f"  {'-' * 50}")
    print(f"  {YELLOW}WARNING: Blindly resets state. Does NOT validate "
          f"health.{RESET}")
    print(f"  Use FAILBACK for normal operations.")

    show_state()

    confirm = input(
        f"\n  {BOLD}Reset to PRIMARY_ACTIVE?{RESET} (yes/no): "
    ).strip().lower()
    if confirm != "yes":
        print("  Cancelled.")
        return

    invoke_lambda(
        ORCHESTRATOR_FUNCTION_NAME,
        PRIMARY_REGION,
        {"reset_state": True},
    )


def print_stats():
    s = _monitor_stats
    total = s["total"]
    if total == 0:
        print(f"\n  {DIM}No checks recorded yet.{RESET}")
        return
    success_pct = (s["success"] / total * 100) if total > 0 else 0
    print(f"\n  {BOLD}Monitor Stats:{RESET}")
    print(
        f"  Checks: {total} | "
        f"{GREEN}Success: {s['success']}{RESET} | "
        f"{RED}Failed: {s['fail']}{RESET} | "
        f"Availability: {success_pct:.1f}%"
    )
    if s["downtime_seconds"] > 0:
        print(
            f"  {YELLOW}Total downtime: {s['downtime_seconds']:.1f}s{RESET}"
        )
    if _failure_sim_running:
        print(
            f"  {RED}Failure simulation: ACTIVE "
            f"({_targets_deregistered} targets de-registered){RESET}"
        )
    print()


def print_header():
    print(f"\n  {BOLD}{'=' * 62}{RESET}")
    print(f"  {BOLD}  FAILOVER ORCHESTRATOR - OPERATOR CLI{RESET}")
    print(f"  {BOLD}{'=' * 62}{RESET}")
    print(f"  {DIM}Primary: {PRIMARY_REGION}  |  Secondary: {SECONDARY_REGION}{RESET}")
    print(f"  {DIM}Monitor: {MONITOR_URL}{RESET}")
    print()


def show_instructions():
    """Show step-by-step demo instructions."""
    print(f"""
  {BOLD}{'=' * 62}
  FAILOVER DEMO — STEP BY STEP INSTRUCTIONS
  {'=' * 62}{RESET}

  {BOLD}PREREQUISITES:{RESET}
    - Both orchestrator Lambdas running (us-east-1 + us-east-2)
    - Both CloudWatch alarms created and connected to Route 53
    - Both Route 53 health checks showing HEALTHY
    - FAILOVER_MODE set to 'manual' on the orchestrator
    - SNS subscription confirmed (check email)

  {BOLD}STEP 1: VERIFY STEADY STATE{RESET}
    - Start this script: python3 failover_cli.py
    - Confirm the monitor shows {GREEN}+ UP{RESET} every 2 seconds
    - Press Enter, select {BOLD}1{RESET} to view state
    - Confirm: active_region=us-east-1, state=PRIMARY_ACTIVE

  {BOLD}STEP 2: SIMULATE FAILURE{RESET}
    - Press Enter, select {BOLD}6{RESET} (Start failure simulation)
    - This de-registers all targets from the ALB target group
    - ECS will try to register new targets; the sim keeps killing them
    - Press Enter, select {BOLD}m{RESET} to resume the live monitor
    - Watch the monitor — within ~30 seconds you'll see:
      {RED}>>> FAILURE DETECTED{RESET}

  {BOLD}STEP 3: WAIT FOR ORCHESTRATOR DETECTION (~3 MINUTES){RESET}
    - The orchestrator Lambda runs every minute
    - Minute 1: consecutive_failures = 1 (WARNING email)
    - Minute 2: consecutive_failures = 2 (WARNING email)
    - Minute 3: consecutive_failures = 3 — threshold reached
    - Since FAILOVER_MODE=manual, you get:
      "FAILOVER RECOMMENDED" email with execute command

  {BOLD}STEP 4: EXECUTE FAILOVER{RESET}
    - Press Enter, select {BOLD}2{RESET} (Execute failover)
    - Confirm region us-east-1, type 'yes'
    - The Lambda flips DNS to us-east-2
    - Watch the monitor — within ~10-30 seconds:
      {GREEN}>>> RECOVERED{RESET} at HH:MM:SS | Downtime: XXXs

  {BOLD}STEP 5: STOP THE FAILURE SIMULATION{RESET}
    - Press Enter, select {BOLD}7{RESET} (Stop failure simulation)
    - ECS will re-register targets in us-east-1 automatically
    - us-east-1 is still latched (traffic stays on us-east-2)

  {BOLD}STEP 6: VIEW RESULTS{RESET}
    - Press Enter, select {BOLD}5{RESET} (Show monitor stats)
    - Shows: total checks, success/fail, availability %, downtime
    - Press Enter, select {BOLD}1{RESET} (View state)
    - Confirm: active_region=us-east-2, latch=True

  {BOLD}STEP 7: FAILBACK (RETURN TO us-east-1){RESET}
    - Switchover Aurora back to us-east-1 first (if it was promoted)
    - Press Enter, select {BOLD}3{RESET} (Execute failback)
    - Target: us-east-1, confirm Aurora is switched
    - The Lambda validates health and moves traffic back
    - Monitor shows traffic returning to us-east-1

  {BOLD}KEY POINTS FOR YOUR AUDIENCE:{RESET}
    - The app stays up during failover (us-east-2 serves traffic)
    - The latch prevents flip-flop (us-east-1 stays marked down)
    - Aurora promotion is manual (operator gets exact CLI commands)
    - The orchestrator auto-detects when Aurora is promoted
    - Failback requires explicit operator action (safety)

  {BOLD}IF SOMETHING GOES WRONG:{RESET}
    - Press Enter, select {BOLD}4{RESET} (Reset state) to start over
    - Check CloudWatch Logs for the Lambda in the failing region
    - Verify Route 53 health checks are both healthy before retesting
""")


def show_menu():
    sim_status = (
        f"{RED}ACTIVE{RESET}" if _failure_sim_running
        else f"{GREEN}inactive{RESET}"
    )
    print(f"\n  {BOLD}Operations:{RESET}")
    print(f"    {BOLD}1{RESET} - View current state")
    print(f"    {BOLD}2{RESET} - Execute failover     {DIM}(manual mode trigger){RESET}")
    print(f"    {BOLD}3{RESET} - Execute failback      {DIM}(return to primary){RESET}")
    print(f"    {BOLD}4{RESET} - Reset state            {DIM}(emergency only){RESET}")
    print(f"    {BOLD}5{RESET} - Show monitor stats")
    print(f"    {BOLD}6{RESET} - Start failure sim      [status: {sim_status}]")
    print(f"    {BOLD}7{RESET} - Stop failure sim")
    print(f"    {BOLD}h{RESET} - Show demo instructions")
    print(f"    {BOLD}m{RESET} - Resume live monitor")
    print(f"    {BOLD}q{RESET} - Quit")
    print()


def main():
    clear_screen()
    print_header()

    print(f"  {BOLD}Starting live health monitor...{RESET}")
    print(f"  {DIM}Press Enter at any time to open the operations menu{RESET}")
    print(f"  {DIM}Press Enter then 'h' for step-by-step demo instructions{RESET}")
    print(f"  {DIM}Monitoring: {MONITOR_URL}{RESET}")
    print()

    init_region_ips()
    start_monitor()

    try:
        while True:
            try:
                input()
            except EOFError:
                break

            pause_monitor()
            time.sleep(0.3)

            print(f"\n  {BOLD}{'_' * 50}{RESET}")
            show_menu()

            choice = input(f"  Select [1-7, h, m, q]: ").strip().lower()

            if choice == "1":
                show_state()
                input(f"\n  Press Enter to continue...")
            elif choice == "2":
                do_failover()
                input(f"\n  Press Enter to continue...")
            elif choice == "3":
                do_failback()
                input(f"\n  Press Enter to continue...")
            elif choice == "4":
                do_reset()
                input(f"\n  Press Enter to continue...")
            elif choice == "5":
                print_stats()
                input(f"\n  Press Enter to continue...")
            elif choice == "6":
                start_failure_sim()
                input(f"\n  Press Enter to continue...")
            elif choice == "7":
                stop_failure_sim()
                input(f"\n  Press Enter to continue...")
            elif choice == "h":
                show_instructions()
                input(f"\n  Press Enter to continue...")
            elif choice == "q":
                if _failure_sim_running:
                    stop_failure_sim()
                break
            elif choice == "m":
                pass
            else:
                print(f"  Invalid choice.")

            print(f"\n  {DIM}Resuming live monitor...{RESET}\n")
            resume_monitor()

    except KeyboardInterrupt:
        pass
    finally:
        if _failure_sim_running:
            stop_failure_sim()
        stop_monitor()
        print_stats()
        print(f"  {DIM}Bye.{RESET}\n")


if __name__ == "__main__":
    main()
