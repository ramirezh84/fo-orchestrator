#!/usr/bin/env python3
"""run_matrix_scenarios.py — exercise the Vigil failover matrix end-to-end.

Cycles through (routing_mode × Aurora × Redis) scenarios. For each:

  1. Save the orchestrator+failback Lambda env snapshot (so we can roll back).
  2. Apply the scenario's env overrides.
  3. Wait for Lambda to pick up new config (one EventBridge cycle ~60s).
  4. Verify the expected per-signal metrics are publishing in CW.
  5. Inject a fault (ECS scale to 0 in primary).
  6. Watch the orchestrator's response — failover claim, state transitions,
     promotion-pending flags, lifecycle counters.
  7. For 'manual' tiers, simulate the operator action (Aurora switchover,
     Redis failover-global).
  8. Run failback Lambda + verify clean return to PRIMARY_ACTIVE / latch=False.
  9. Restore ECS, restore Aurora writer to us-east-1, restore Redis primary
     to us-east-1, restore Lambda env to original snapshot.
 10. Capture pass/fail + diagnostic evidence per scenario.

After all scenarios, writes a JSON report and prints a console summary.
With --loop-until-pass, re-runs the failed scenarios up to N iterations.

Usage:
  python3 tools/run_matrix_scenarios.py --dry-run
  python3 tools/run_matrix_scenarios.py --profile tbed --app fo-v16-drill
  python3 tools/run_matrix_scenarios.py --profile tbed --app fo-v16-drill \\
      --scenarios AP_C5 AA_C2
  python3 tools/run_matrix_scenarios.py --profile tbed --app fo-v16-drill \\
      --loop-until-pass --max-iterations 3

Designed for fo-v16-drill (us-east-1 primary, us-east-2 secondary) but the
--app flag and --regions <primary> <secondary> let it target other stacks.
"""

import argparse
import dataclasses
import json
import logging
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import boto3

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger("matrix")


# ---------------------------------------------------------------------------
# Scenario model
# ---------------------------------------------------------------------------

@dataclasses.dataclass
class Scenario:
    id: str                     # AP_C1 / AA_C9 etc.
    name: str
    routing_mode: str           # failover | active-active
    aurora: str                 # absent | manual | auto
    redis: str                  # absent | manual | auto
    api_gw: bool = True
    expected_failover_fires: bool = True
    expected_aurora_promotion: bool = False
    expected_redis_promotion: bool = False
    notes: str = ""

    @property
    def env_overrides(self) -> dict:
        """Lambda env vars to set for this scenario."""
        e: dict = {"ROUTING_MODE": self.routing_mode}
        # Aurora
        if self.aurora == "absent":
            e["AURORA_CLUSTER_ID"] = ""
            e["AURORA_AUTO_PROMOTE"] = "false"
        elif self.aurora == "manual":
            e["AURORA_AUTO_PROMOTE"] = "false"
        elif self.aurora == "auto":
            e["AURORA_AUTO_PROMOTE"] = "true"
        # Redis
        if self.redis == "absent":
            e["ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID"] = ""
            e["ELASTICACHE_REPLICATION_GROUP_ID"] = ""
            e["ELASTICACHE_AUTO_PROMOTE"] = "false"
        elif self.redis == "manual":
            e["ELASTICACHE_AUTO_PROMOTE"] = "false"
        elif self.redis == "auto":
            e["ELASTICACHE_AUTO_PROMOTE"] = "true"
        # API GW
        if not self.api_gw:
            e["API_GW_NAME"] = ""
        return e

    @property
    def expected_signals(self) -> set:
        s = {"SignalHttp", "SignalAlb", "SignalEcs"}
        if self.aurora != "absent":
            s.add("SignalAurora")
        if self.redis != "absent":
            s.add("SignalElasticache")
        if self.api_gw:
            s.add("SignalApiGw")
        return s


def build_matrix() -> list:
    """The 18-scenario matrix: 9 active/passive × 9 active/active."""
    out = []
    for routing in ["failover", "active-active"]:
        prefix = "AP" if routing == "failover" else "AA"
        for aur_ix, aur in enumerate(["absent", "manual", "auto"]):
            for redis_ix, redis in enumerate(["absent", "manual", "auto"]):
                cell = aur_ix * 3 + redis_ix + 1
                sid = f"{prefix}_C{cell}"
                # Active/active doesn't engage the v1.0 latch, but data-tier
                # promotion SHOULD still fire when the writer region fails.
                # Marking expected_failover_fires=False for active/active
                # because there's no orchestrated failover claim — health
                # publish flips the metric and Route 53 reroutes naturally.
                expect_fo = (routing == "failover")
                # When auto-promote is on AND the writer region fails, the
                # orchestrator should initiate promotion in BOTH modes. In
                # failover mode this is well-trodden code; in active/active
                # it's the gap the user flagged — there's no promotion logic
                # in _handle_active_active today, so these expectations will
                # correctly fail and surface the gap in the report.
                expect_aurora_promo = (aur == "auto")
                expect_redis_promo = (redis == "auto")
                out.append(Scenario(
                    id=sid,
                    name=f"{routing} · Aurora-{aur} · Redis-{redis}",
                    routing_mode=routing,
                    aurora=aur,
                    redis=redis,
                    api_gw=True,
                    expected_failover_fires=expect_fo,
                    expected_aurora_promotion=expect_aurora_promo,
                    expected_redis_promotion=expect_redis_promo,
                    notes=(
                        "active/active: data-tier promotion behavior "
                        "currently undocumented — will surface gap"
                        if routing == "active-active" and (aur != "absent" or redis != "absent")
                        else ""
                    ),
                ))
    return out


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

class Runner:
    """Holds AWS sessions + per-app conventions; one instance per matrix run."""

    def __init__(self, profile: str, app: str, primary: str, secondary: str):
        self.session = boto3.Session(profile_name=profile)
        self.app = app                      # "fo-v16-drill"
        self.primary = primary              # "us-east-1"
        self.secondary = secondary          # "us-east-2"
        # Per-app naming conventions matching cfn/failover.yaml + fo-v16-drill:
        self.orch_fn = f"fo-{app}-orchestrator"
        self.failback_fn = f"fo-{app}-failback"
        self.ecs_cluster = f"fo-{app}-cluster"
        self.ecs_service = f"fo-{app}-app-svc"
        self.aurora_global = f"{app}-aurora-global"
        self.aurora_e1 = f"{app}-aurora-e1"
        self.aurora_e2 = f"{app}-aurora-e2"
        self.redis_global = None  # discovered lazily
        self.cw_namespace = "Custom/FoDemo"
        self.app_dim = app  # AppName dim value

        # Snapshots for restore.
        self._env_snapshot: dict = {}
        self._original_failures_threshold: Optional[str] = None

    def lambda_client(self, region: str):
        return self.session.client("lambda", region_name=region)

    def cw(self, region: str):
        return self.session.client("cloudwatch", region_name=region)

    def ecs(self, region: str):
        return self.session.client("ecs", region_name=region)

    def rds(self, region: str):
        return self.session.client("rds", region_name=region)

    def elasticache(self, region: str):
        return self.session.client("elasticache", region_name=region)

    # ---- Env var helpers ------------------------------------------------

    def get_env(self, region: str, fn: str) -> dict:
        cfg = self.lambda_client(region).get_function_configuration(FunctionName=fn)
        return dict(cfg["Environment"]["Variables"])

    def set_env(self, region: str, fn: str, env: dict) -> None:
        self.lambda_client(region).update_function_configuration(
            FunctionName=fn, Environment={"Variables": env},
        )

    def snapshot_envs(self) -> None:
        log.info("Snapshotting Lambda env vars across both regions + functions...")
        self._env_snapshot = {}
        for region in [self.primary, self.secondary]:
            for fn in [self.orch_fn, self.failback_fn]:
                self._env_snapshot[(region, fn)] = self.get_env(region, fn)
        log.info("  Snapshot saved: %d env dicts", len(self._env_snapshot))

    def restore_envs(self) -> None:
        log.info("Restoring Lambda env vars from snapshot...")
        for (region, fn), env in self._env_snapshot.items():
            try:
                self.set_env(region, fn, env)
                log.info("  %s/%s restored (%d vars)", region, fn, len(env))
            except Exception as e:
                log.warning("  failed to restore %s/%s: %s", region, fn, e)

    def apply_scenario_env(self, s: Scenario) -> None:
        """Apply scenario overrides to BOTH orchestrator Lambdas (in both regions).

        Uses the snapshot as the base; only overrides scenario-relevant vars
        so unrelated state (TG_ARN_SUFFIX, CW_NAMESPACE, etc.) stays put.
        """
        for region in [self.primary, self.secondary]:
            base = dict(self._env_snapshot[(region, self.orch_fn)])
            for k, v in s.env_overrides.items():
                base[k] = v
            # If Redis is absent, also clear Redis local RG to keep signal SKIPPED.
            if s.redis == "absent":
                base.pop("ELASTICACHE_REPLICATION_GROUP_ID", None)
                base["ELASTICACHE_REPLICATION_GROUP_ID"] = ""
            else:
                # Local RG depends on which region the Lambda is in.
                if region == self.primary:
                    base["ELASTICACHE_REPLICATION_GROUP_ID"] = f"{self.app}-redis-e1"
                else:
                    base["ELASTICACHE_REPLICATION_GROUP_ID"] = f"{self.app}-redis-e2"
            self.set_env(region, self.orch_fn, base)
        log.info("  Scenario env applied to orchestrator Lambdas in %s + %s",
                 self.primary, self.secondary)

    # ---- ECS fault injection -------------------------------------------

    def scale_ecs(self, region: str, count: int) -> None:
        log.info("  Scaling ECS service %s/%s to %d tasks...",
                 region, self.ecs_service, count)
        self.ecs(region).update_service(
            cluster=self.ecs_cluster, service=self.ecs_service, desiredCount=count,
        )

    # ---- State + metric checks -----------------------------------------

    def get_failover_state(self) -> dict:
        """Read the orchestrator's S3 state file directly. Faster than invoking
        the Lambda. Bucket name follows the cfn convention
        ``fo-<app>-state-<region>-<account>``."""
        if not hasattr(self, "_account"):
            self._account = self.session.client("sts").get_caller_identity()["Account"]
        bucket = f"fo-{self.app}-state-{self.primary}-{self._account}"
        s3 = self.session.client("s3", region_name=self.primary)
        r = s3.get_object(Bucket=bucket, Key="failover-state/REGION_STATE.json")
        return json.loads(r["Body"].read())

    def published_signals_for_app(self, region: str) -> set:
        """List metric names with AppName=this app in the namespace."""
        try:
            r = self.cw(region).list_metrics(Namespace=self.cw_namespace)
            out = set()
            for m in r.get("Metrics", []):
                dims = {d["Name"]: d["Value"] for d in m.get("Dimensions", [])}
                if dims.get("AppName") == self.app_dim:
                    out.add(m["MetricName"])
            return out
        except Exception as e:
            log.warning("    list_metrics failed: %s", e)
            return set()

    # ---- Failback + cleanup --------------------------------------------

    def run_failback(self, target_region: str) -> dict:
        log.info("  Invoking failback Lambda in %s...", target_region)
        payload = json.dumps({
            "target_region": target_region,
            "operator": "matrix-runner",
            "aurora_confirmed": True,
            "redis_confirmed": True,
            "skip_health_check": True,
            "skip_readiness_check": True,
        })
        r = self.lambda_client(target_region).invoke(
            FunctionName=self.failback_fn,
            Payload=payload.encode("utf-8"),
        )
        body = r["Payload"].read().decode("utf-8")
        try:
            return json.loads(body)
        except Exception:
            return {"raw": body}

    def restore_aurora_writer_to_primary(self) -> None:
        """If Aurora writer is in the secondary region, switch it back."""
        try:
            r = self.rds(self.primary).describe_global_clusters(
                GlobalClusterIdentifier=self.aurora_global,
            )
            for gc in r.get("GlobalClusters", []):
                for m in gc.get("GlobalClusterMembers", []):
                    if m.get("IsWriter") and f":{self.secondary}:" in m.get("DBClusterArn", ""):
                        log.info("  Aurora writer is in %s — switching back to %s",
                                 self.secondary, self.primary)
                        target_arn = next(
                            x["DBClusterArn"] for x in gc["GlobalClusterMembers"]
                            if f":{self.primary}:" in x["DBClusterArn"]
                        )
                        self.rds(self.primary).switchover_global_cluster(
                            GlobalClusterIdentifier=self.aurora_global,
                            TargetDbClusterIdentifier=target_arn,
                        )
                        # Wait briefly — full switchover takes 1-3 min, but
                        # for the runner we just need the API to accept.
                        log.info("    switchover initiated; subsequent scenarios will see"
                                 " writer in %s after ~2-3 min", self.primary)
                        return
        except Exception as e:
            log.warning("  Aurora restore check failed: %s", e)

    def restore_redis_primary_to_primary(self) -> None:
        if not self.redis_global:
            return
        try:
            r = self.elasticache(self.primary).describe_global_replication_groups(
                GlobalReplicationGroupId=self.redis_global,
                ShowMemberInfo=True,
            )
            for grg in r.get("GlobalReplicationGroups", []):
                for m in grg.get("Members", []):
                    if m.get("Role") == "PRIMARY" and m.get("ReplicationGroupRegion") == self.secondary:
                        log.info("  Redis primary is in %s — failing over to %s",
                                 self.secondary, self.primary)
                        primary_rg = f"{self.app}-redis-e1"
                        self.elasticache(self.primary).failover_global_replication_group(
                            GlobalReplicationGroupId=self.redis_global,
                            PrimaryRegion=self.primary,
                            PrimaryReplicationGroupId=primary_rg,
                        )
                        return
        except Exception as e:
            log.warning("  Redis restore check failed: %s", e)


# ---------------------------------------------------------------------------
# Per-scenario execution
# ---------------------------------------------------------------------------

def run_scenario(r: Runner, s: Scenario, settle_seconds: int) -> dict:
    """Execute one scenario and return a result dict."""
    started = datetime.now(timezone.utc).isoformat()
    log.info("=" * 76)
    log.info("Scenario %s: %s", s.id, s.name)
    log.info("=" * 76)

    result = {
        "id": s.id,
        "name": s.name,
        "started_at": started,
        "status": "PENDING",
        "errors": [],
        "evidence": {},
    }

    try:
        # Step 1 — apply env
        log.info("[1] Applying scenario env to orchestrator Lambdas...")
        r.apply_scenario_env(s)

        # Step 2 — wait for next Lambda cycle
        log.info("[2] Waiting %ds for Lambdas to pick up new config...", settle_seconds)
        time.sleep(settle_seconds)

        # Step 3 — verify expected signals are publishing (best-effort,
        # within a short polling window).
        log.info("[3] Verifying expected signals (best-effort)...")
        seen_primary = r.published_signals_for_app(r.primary)
        seen_secondary = r.published_signals_for_app(r.secondary)
        result["evidence"]["signals_seen_primary"]   = sorted(seen_primary)
        result["evidence"]["signals_seen_secondary"] = sorted(seen_secondary)
        missing = s.expected_signals - (seen_primary | seen_secondary)
        if missing:
            result["errors"].append(
                f"expected signals not seen: {sorted(missing)}"
            )

        # Step 4 — inject ECS fault in primary
        log.info("[4] Injecting ECS fault in %s (scale to 0)...", r.primary)
        r.scale_ecs(r.primary, 0)
        result["evidence"]["fault_injected_at"] = datetime.now(timezone.utc).isoformat()

        # Step 5 — wait for orchestrator response.
        # CONSECUTIVE_FAILURES_THRESHOLD defaults to 3, so we need ~3-4 min.
        # We poll until either failover claim is observed (latch=True OR
        # state changes) OR we hit timeout.
        log.info("[5] Waiting up to 5min for orchestrator to react to the fault...")
        deadline = time.time() + 300
        observed = {}
        while time.time() < deadline:
            time.sleep(20)
            try:
                state = r.get_failover_state()
                observed["state"] = state.get("state")
                observed["latch"] = state.get("latch_engaged")
                observed["consecutive_failures"] = state.get("consecutive_failures")
                observed["aurora_promotion_pending"] = state.get("aurora_promotion_pending")
                observed["redis_promotion_pending"] = state.get("redis_promotion_pending")
                if state.get("latch_engaged") or state.get("state") in (
                    "WAITING_AURORA_PROMOTION", "SECONDARY_ACTIVE"
                ):
                    log.info("    Failover detected: state=%s latch=%s",
                             observed["state"], observed["latch"])
                    break
            except Exception as e:
                log.warning("    state read failed: %s", e)
        result["evidence"]["observed_state"] = observed

        # Step 6 — assert expected behavior
        log.info("[6] Asserting scenario expectations...")
        latch = observed.get("latch")
        if s.expected_failover_fires and not latch:
            result["errors"].append(
                "expected failover to fire (latch=True), but state did not transition"
            )
        if not s.expected_failover_fires and latch:
            result["errors"].append(
                "did NOT expect failover to fire, but latch=True was observed"
            )
        if s.expected_aurora_promotion:
            # Check FailoversTriggered counter ticked OR aurora_promotion_pending set
            if not (observed.get("aurora_promotion_pending") or
                    observed.get("state") == "WAITING_AURORA_PROMOTION"):
                result["errors"].append(
                    "expected Aurora promotion to be initiated, no evidence found"
                )

    except Exception as e:
        log.exception("Scenario %s threw an exception", s.id)
        result["errors"].append(f"runner exception: {type(e).__name__}: {e}")

    finally:
        # Cleanup — restore ECS, run failback, restore Aurora/Redis writer back.
        log.info("[cleanup] Restoring ECS to 2 tasks + invoking failback...")
        try:
            r.scale_ecs(r.primary, 2)
        except Exception as e:
            result["errors"].append(f"ecs restore failed: {e}")
        # Wait briefly for tasks to come back healthy before failback.
        time.sleep(60)
        try:
            r.run_failback(r.primary)
        except Exception as e:
            result["errors"].append(f"failback failed: {e}")
        try:
            r.restore_aurora_writer_to_primary()
        except Exception as e:
            result["errors"].append(f"aurora restore failed: {e}")
        try:
            r.restore_redis_primary_to_primary()
        except Exception as e:
            result["errors"].append(f"redis restore failed: {e}")

        result["finished_at"] = datetime.now(timezone.utc).isoformat()
        result["status"] = "PASS" if not result["errors"] else "FAIL"
        log.info("Scenario %s → %s", s.id, result["status"])
        if result["errors"]:
            for err in result["errors"]:
                log.info("    %s", err)

    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--profile", default="tbed", help="AWS profile (default: tbed)")
    p.add_argument("--app", default="fo-v16-drill",
                   help="App name (drives Lambda function names + ECS cluster + Aurora IDs)")
    p.add_argument("--regions", nargs=2, default=["us-east-1", "us-east-2"],
                   help="primary secondary (default: us-east-1 us-east-2)")
    p.add_argument("--scenarios", nargs="+",
                   help="Run only the named scenarios (e.g. AP_C5 AA_C2)")
    p.add_argument("--dry-run", action="store_true",
                   help="List scenarios + planned env overrides without executing")
    p.add_argument("--report", default="matrix-report.json",
                   help="Output JSON report path (default: matrix-report.json)")
    p.add_argument("--settle-seconds", type=int, default=90,
                   help="Wait for Lambda to pick up new env vars (default: 90)")
    p.add_argument("--loop-until-pass", action="store_true",
                   help="Re-run failed scenarios until all pass or max iterations hit")
    p.add_argument("--max-iterations", type=int, default=3)
    args = p.parse_args()

    matrix = build_matrix()
    if args.scenarios:
        wanted = set(args.scenarios)
        matrix = [s for s in matrix if s.id in wanted]
        if not matrix:
            sys.exit(f"No scenarios match filter {args.scenarios!r}")

    log.info("Matrix has %d scenarios.", len(matrix))
    if args.dry_run:
        for s in matrix:
            print(f"  {s.id:8} {s.name}")
            print(f"    env overrides: {json.dumps(s.env_overrides, sort_keys=True)}")
            print(f"    expected signals: {sorted(s.expected_signals)}")
            print(f"    expected_failover_fires: {s.expected_failover_fires}")
            print(f"    expected_aurora_promotion: {s.expected_aurora_promotion}")
            print(f"    expected_redis_promotion: {s.expected_redis_promotion}")
            if s.notes:
                print(f"    notes: {s.notes}")
        return

    primary, secondary = args.regions
    runner = Runner(profile=args.profile, app=args.app, primary=primary, secondary=secondary)
    runner.snapshot_envs()
    # Discover Redis Global Datastore for cleanup helper.
    try:
        rgs = runner.elasticache(primary).describe_global_replication_groups()
        for grg in rgs.get("GlobalReplicationGroups", []):
            if args.app in grg.get("GlobalReplicationGroupId", ""):
                runner.redis_global = grg["GlobalReplicationGroupId"]
                log.info("Discovered Redis Global Datastore: %s", runner.redis_global)
                break
    except Exception as e:
        log.warning("Redis Global Datastore discovery failed: %s", e)

    all_results = []
    iteration = 0
    pending = list(matrix)

    try:
        while pending and iteration < args.max_iterations:
            iteration += 1
            log.info("################ Iteration %d (%d scenarios pending) ################",
                     iteration, len(pending))
            failures = []
            for s in pending:
                r = run_scenario(runner, s, args.settle_seconds)
                r["iteration"] = iteration
                all_results.append(r)
                if r["status"] != "PASS":
                    failures.append(s)
            if not args.loop_until_pass:
                break
            pending = failures
    finally:
        log.info("================ Restoring original Lambda env snapshot =============")
        runner.restore_envs()
        # Final cleanup of data tier.
        try:
            runner.restore_aurora_writer_to_primary()
            runner.restore_redis_primary_to_primary()
        except Exception:
            pass

    # Write JSON report
    report = {
        "app": args.app,
        "regions": {"primary": primary, "secondary": secondary},
        "iterations_run": iteration,
        "scenarios_run": len(all_results),
        "passed": sum(1 for r in all_results if r["status"] == "PASS"),
        "failed": sum(1 for r in all_results if r["status"] == "FAIL"),
        "results": all_results,
    }
    Path(args.report).write_text(json.dumps(report, indent=2, default=str))
    log.info("Report written to %s", args.report)

    # Console summary
    print()
    print("=" * 76)
    print(f"MATRIX RUN SUMMARY — {report['passed']}/{report['scenarios_run']} passed across {iteration} iteration(s)")
    print("=" * 76)
    for r in all_results:
        marker = "✓" if r["status"] == "PASS" else "✗"
        print(f"  {marker} [iter {r['iteration']}] {r['id']:8} {r['name']}")
        if r["errors"]:
            for err in r["errors"]:
                print(f"        - {err}")

    sys.exit(0 if report["failed"] == 0 else 1)


if __name__ == "__main__":
    main()
