#!/usr/bin/env python3
"""validate_dashboard.py — drift check between generator output and deployed dashboard.

Fetches a deployed CloudWatch dashboard via ``cloudwatch:GetDashboard``,
regenerates the dashboard from a YAML config, and reports whether they match.

Useful as a pre-deploy gate or as a CI guardrail: any time someone hand-edits
the dashboard in the AWS console, this script flags the divergence so the
config can be updated to match (or the manual edit reverted).

Exit code 0 = no drift. Exit code 1 = drift detected. Exit code 2 = error
(network, missing dashboard, etc.).

Usage:
    python3 tools/validate_dashboard.py --config <yaml>
    python3 tools/validate_dashboard.py --config <yaml> --region us-east-1
    python3 tools/validate_dashboard.py --config <yaml> --profile tbed
"""

import argparse
import json
import sys
from pathlib import Path

# Reuse the generator's loader + builder.
sys.path.insert(0, str(Path(__file__).resolve().parent))
import generate_dashboard as gd  # noqa: E402


def _normalize(d: dict) -> dict:
    """Normalize a dashboard JSON for stable diffing.

    CloudWatch may add cosmetic fields (period defaults, view defaults) on
    GetDashboard. We canonicalize both sides so non-meaningful differences
    don't show up as drift. Keep this conservative — only strip fields we
    actively know are noise.
    """
    if not isinstance(d, dict):
        return d
    widgets = d.get("widgets", [])
    return {"widgets": [_normalize_widget(w) for w in widgets]}


def _normalize_widget(w: dict) -> dict:
    """Drop fields that CW adds/strips during put/get round-trip."""
    out = dict(w)
    props = dict(out.get("properties", {}))
    # CloudWatch returns these even when not set on PUT.
    for k in ("liveData", "setPeriodToTimeRange", "sparkline", "trend"):
        props.pop(k, None)
    out["properties"] = props
    return out


def _print_diff_summary(local: dict, remote: dict) -> None:
    """Best-effort human-readable diff so the operator sees what changed."""
    local_titles = [w.get("properties", {}).get("title")
                    or w.get("properties", {}).get("markdown", "")[:40]
                    for w in local.get("widgets", [])]
    remote_titles = [w.get("properties", {}).get("title")
                     or w.get("properties", {}).get("markdown", "")[:40]
                     for w in remote.get("widgets", [])]
    if local_titles != remote_titles:
        print("Widget titles differ:", file=sys.stderr)
        print(f"  local  ({len(local_titles)}): {local_titles}", file=sys.stderr)
        print(f"  remote ({len(remote_titles)}): {remote_titles}", file=sys.stderr)
    else:
        # Same titles, deeper field diff. Print the first widget that differs.
        for i, (lw, rw) in enumerate(zip(local["widgets"], remote["widgets"])):
            if lw != rw:
                print(f"\nWidget {i} ({local_titles[i]!r}) differs:", file=sys.stderr)
                print(f"  local : {json.dumps(lw, indent=2, sort_keys=True)}",
                      file=sys.stderr)
                print(f"  remote: {json.dumps(rw, indent=2, sort_keys=True)}",
                      file=sys.stderr)
                return


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, metavar="FILE",
                        help="Path to YAML config (same as generate_dashboard.py).")
    parser.add_argument("--region", metavar="REGION",
                        help="Region where the dashboard is deployed (default: primary_region).")
    parser.add_argument("--profile", metavar="NAME", help="AWS profile.")
    args = parser.parse_args()

    cfg = gd.load_config(args.config)
    expected = _normalize(gd.build_dashboard(cfg))
    region = args.region or cfg["primary_region"]
    name = cfg["dashboard_name"]

    try:
        import boto3
    except ImportError:
        sys.exit("boto3 is required: pip install boto3")
    session = boto3.Session(profile_name=args.profile) if args.profile else boto3.Session()
    cw = session.client("cloudwatch", region_name=region)

    try:
        resp = cw.get_dashboard(DashboardName=name)
    except cw.exceptions.ResourceNotFound:
        print(f"[error] Dashboard '{name}' does not exist in {region}.", file=sys.stderr)
        print(f"        Run: python3 tools/generate_dashboard.py --config {args.config} --deploy",
              file=sys.stderr)
        sys.exit(2)
    except Exception as e:
        print(f"[error] Failed to fetch dashboard: {type(e).__name__}: {e}", file=sys.stderr)
        sys.exit(2)

    deployed = _normalize(json.loads(resp["DashboardBody"]))

    if expected == deployed:
        print(f"OK — dashboard '{name}' in {region} matches generator output.")
        sys.exit(0)

    print(f"DRIFT — dashboard '{name}' in {region} differs from generator output.",
          file=sys.stderr)
    _print_diff_summary(expected, deployed)
    print("\nFix: re-run with --deploy to overwrite, OR update the YAML config "
          "to match the manual changes.", file=sys.stderr)
    sys.exit(1)


if __name__ == "__main__":
    main()
