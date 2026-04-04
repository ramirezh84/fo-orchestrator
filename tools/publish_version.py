#!/usr/bin/env python3
"""
Publish a new Lambda version and update an alias.

Usage:
  # Publish current code and create/update alias
  python3 tools/publish_version.py --alias v1.2

  # Publish to specific region
  python3 tools/publish_version.py --alias v1.2 --region us-west-1

  # Publish from a specific git ref (builds zip from that commit)
  python3 tools/publish_version.py --alias v1.0 --git-ref v1.0

  # Update the 'active' alias to point to the same version as v1.2
  python3 tools/publish_version.py --alias active --copy-from v1.2
"""

import argparse
import os
import subprocess
import sys
import tempfile
import zipfile

import boto3
from botocore.exceptions import ClientError

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PRIMARY_REGION = "us-west-1"
SECONDARY_REGION = "us-west-2"
BOTH_REGIONS = [PRIMARY_REGION, SECONDARY_REGION]

ORCHESTRATOR_NAME = "fo-demo-orchestrator"
FAILBACK_NAME = "fo-demo-failback"


def build_zip(base_name, includes, git_ref=None):
    """Build a Lambda deployment zip. Optionally from a specific git ref."""
    tmp = tempfile.NamedTemporaryFile(suffix=".zip", delete=False)
    tmp.close()

    if git_ref:
        # Build from a specific git commit
        work_dir = tempfile.mkdtemp()
        subprocess.run(
            ["git", "archive", "--format=tar", git_ref],
            cwd=PROJECT_ROOT, capture_output=True, check=True
        )
        # Extract and build zip from the archive
        result = subprocess.run(
            ["git", "archive", git_ref] + includes,
            cwd=PROJECT_ROOT, capture_output=True
        )
        if result.returncode != 0:
            # Fallback: use current working tree
            print(f"  Warning: git archive failed for {git_ref}, using current code")
            git_ref = None

    with zipfile.ZipFile(tmp.name, "w", zipfile.ZIP_DEFLATED) as zf:
        for inc in includes:
            full_path = os.path.join(PROJECT_ROOT, inc)
            if os.path.isdir(full_path):
                for root, _dirs, files in os.walk(full_path):
                    for fname in files:
                        if fname.endswith(".py"):
                            fp = os.path.join(root, fname)
                            arc = os.path.relpath(fp, PROJECT_ROOT)
                            zf.write(fp, arc)
            elif os.path.isfile(full_path):
                zf.write(full_path, inc)

    return tmp.name


def publish_and_alias(function_name, zip_path, alias_name, region):
    """Upload code, publish version, create/update alias."""
    lam = boto3.client("lambda", region_name=region)

    # Upload code
    print(f"  Uploading code to {function_name} in {region}...")
    with open(zip_path, "rb") as f:
        lam.update_function_code(FunctionName=function_name, ZipFile=f.read())

    # Wait for update
    print(f"  Waiting for update to complete...")
    waiter = lam.get_waiter("function_updated_v2")
    waiter.wait(FunctionName=function_name)

    # Publish version
    print(f"  Publishing version...")
    resp = lam.publish_version(FunctionName=function_name)
    version = resp["Version"]
    print(f"  Published version {version}")

    # Create or update alias
    _create_or_update_alias(lam, function_name, alias_name, version)
    print(f"  Alias '{alias_name}' -> version {version}")

    return version


def copy_alias(function_name, alias_name, copy_from, region):
    """Point alias_name to the same version as copy_from."""
    lam = boto3.client("lambda", region_name=region)

    # Get the version that copy_from points to
    try:
        resp = lam.get_alias(FunctionName=function_name, Name=copy_from)
        version = resp["FunctionVersion"]
    except ClientError as e:
        print(f"  Error: alias '{copy_from}' not found on {function_name} in {region}")
        raise

    _create_or_update_alias(lam, function_name, alias_name, version)
    print(f"  Alias '{alias_name}' -> version {version} (copied from '{copy_from}')")


def _create_or_update_alias(lam, function_name, alias_name, version):
    """Create alias if it doesn't exist, otherwise update it."""
    try:
        lam.update_alias(
            FunctionName=function_name,
            Name=alias_name,
            FunctionVersion=version,
        )
    except ClientError as e:
        if "ResourceNotFoundException" in str(e):
            lam.create_alias(
                FunctionName=function_name,
                Name=alias_name,
                FunctionVersion=version,
            )
        else:
            raise


def main():
    parser = argparse.ArgumentParser(description="Publish Lambda version and update alias")
    parser.add_argument("--alias", required=True, help="Alias name (e.g., v1.0, v1.2, active)")
    parser.add_argument("--region", help="Deploy to specific region (default: both)")
    parser.add_argument("--git-ref", help="Build zip from a specific git ref")
    parser.add_argument("--copy-from", help="Copy version pointer from another alias (no code upload)")
    args = parser.parse_args()

    regions = [args.region] if args.region else BOTH_REGIONS

    if args.copy_from:
        # Just update alias pointer, no code upload
        for region in regions:
            print(f"\nCopying alias in {region}:")
            copy_alias(ORCHESTRATOR_NAME, args.alias, args.copy_from, region)
            copy_alias(FAILBACK_NAME, args.alias, args.copy_from, region)
        print("\nDone.")
        return

    # Build zips
    print("Building orchestrator zip...")
    orch_zip = build_zip("orchestrator", [
        "failover_orchestrator_v3.py",
        "state_backend.py",
        "ai",
    ], git_ref=args.git_ref)

    print("Building failback zip...")
    fb_zip = build_zip("failback", [
        "manual_failback_v2.py",
        "state_backend.py",
        "ai",
    ], git_ref=args.git_ref)

    # Deploy to each region
    for region in regions:
        print(f"\n{'='*60}")
        print(f"Region: {region}")
        print(f"{'='*60}")
        publish_and_alias(ORCHESTRATOR_NAME, orch_zip, args.alias, region)
        publish_and_alias(FAILBACK_NAME, fb_zip, args.alias, region)

    # Cleanup
    os.unlink(orch_zip)
    os.unlink(fb_zip)
    print(f"\nDone. Alias '{args.alias}' updated in {', '.join(regions)}.")


if __name__ == "__main__":
    main()
