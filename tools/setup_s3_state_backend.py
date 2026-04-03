#!/usr/bin/env python3
"""
S3 State Backend — Infrastructure Setup
==========================================
Creates S3 buckets with versioning and Cross-Region Replication (CRR)
for use as a DynamoDB-free state backend for the failover orchestrator.

Creates:
  1. Source bucket in PRIMARY_REGION (e.g., us-east-1)
  2. Replica bucket in SECONDARY_REGION (e.g., us-east-2)
  3. IAM role for S3 CRR
  4. Replication rule: source → replica
  5. Replication rule: replica → source (bidirectional)

Usage:
  python3 setup_s3_state_backend.py

  # Or override defaults:
  python3 setup_s3_state_backend.py \\
    --primary-region us-west-1 \\
    --secondary-region us-west-2 \\
    --bucket-prefix my-app-failover

After setup, configure Lambda env vars:
  STATE_BACKEND=s3
  STATE_BUCKET=<bucket-name-for-this-region>
  STATE_PREFIX=failover-state/
"""

import argparse
import json
import sys
import time

import boto3
from botocore.exceptions import ClientError


def get_account_id():
    return boto3.client("sts").get_caller_identity()["Account"]


def create_bucket(s3_client, bucket_name, region):
    """Create an S3 bucket if it doesn't exist."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"  Bucket {bucket_name} already exists")
        return False
    except ClientError as e:
        if e.response["Error"]["Code"] not in ("404", "NoSuchBucket"):
            raise

    print(f"  Creating bucket {bucket_name} in {region}...")
    create_kwargs = {"Bucket": bucket_name}
    # us-east-1 doesn't accept LocationConstraint
    if region != "us-east-1":
        create_kwargs["CreateBucketConfiguration"] = {
            "LocationConstraint": region
        }
    s3_client.create_bucket(**create_kwargs)
    print(f"  Bucket {bucket_name} created")
    return True


def enable_versioning(s3_client, bucket_name):
    """Enable versioning (required for CRR)."""
    print(f"  Enabling versioning on {bucket_name}...")
    s3_client.put_bucket_versioning(
        Bucket=bucket_name,
        VersioningConfiguration={"Status": "Enabled"},
    )


def create_replication_role(iam_client, role_name, source_bucket, dest_bucket, account_id):
    """Create IAM role for S3 CRR."""
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "s3.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }

    replication_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetReplicationConfiguration",
                    "s3:ListBucket",
                ],
                "Resource": f"arn:aws:s3:::{source_bucket}",
            },
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObjectVersionForReplication",
                    "s3:GetObjectVersionAcl",
                    "s3:GetObjectVersionTagging",
                ],
                "Resource": f"arn:aws:s3:::{source_bucket}/*",
            },
            {
                "Effect": "Allow",
                "Action": [
                    "s3:ReplicateObject",
                    "s3:ReplicateDelete",
                    "s3:ReplicateTags",
                ],
                "Resource": f"arn:aws:s3:::{dest_bucket}/*",
            },
        ],
    }

    try:
        resp = iam_client.get_role(RoleName=role_name)
        role_arn = resp["Role"]["Arn"]
        print(f"  IAM role {role_name} already exists: {role_arn}")
    except ClientError as e:
        if e.response["Error"]["Code"] != "NoSuchEntity":
            raise
        print(f"  Creating IAM role {role_name}...")
        resp = iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="S3 CRR role for failover state replication",
        )
        role_arn = resp["Role"]["Arn"]
        print(f"  Role created: {role_arn}")

    # Attach inline policy
    policy_name = f"{role_name}-policy"
    iam_client.put_role_policy(
        RoleName=role_name,
        PolicyName=policy_name,
        PolicyDocument=json.dumps(replication_policy),
    )
    print(f"  Policy {policy_name} attached")

    return role_arn


def setup_replication(s3_client, source_bucket, dest_bucket, role_arn, dest_account_id):
    """Configure CRR from source to dest bucket."""
    print(f"  Setting up replication: {source_bucket} → {dest_bucket}...")
    replication_config = {
        "Role": role_arn,
        "Rules": [
            {
                "ID": "failover-state-replication",
                "Priority": 1,
                "Status": "Enabled",
                "Filter": {
                    "Prefix": "failover-state/",
                },
                "Destination": {
                    "Bucket": f"arn:aws:s3:::{dest_bucket}",
                    "StorageClass": "STANDARD",
                    "ReplicationTime": {
                        "Status": "Enabled",
                        "Time": {"Minutes": 15},
                    },
                    "Metrics": {
                        "Status": "Enabled",
                        "EventThreshold": {"Minutes": 15},
                    },
                },
                "DeleteMarkerReplication": {"Status": "Enabled"},
            }
        ],
    }

    s3_client.put_bucket_replication(
        Bucket=source_bucket,
        ReplicationConfiguration=replication_config,
    )
    print(f"  Replication rule configured")


def main():
    parser = argparse.ArgumentParser(description="Set up S3 CRR for failover state backend")
    parser.add_argument("--primary-region", default="us-east-1")
    parser.add_argument("--secondary-region", default="us-east-2")
    parser.add_argument("--bucket-prefix", default="failover-state")
    parser.add_argument("--teardown", action="store_true", help="Remove all resources")
    args = parser.parse_args()

    account_id = get_account_id()
    primary_bucket = f"{args.bucket_prefix}-{args.primary_region}-{account_id}"
    secondary_bucket = f"{args.bucket_prefix}-{args.secondary_region}-{account_id}"

    print(f"\nS3 State Backend Setup")
    print(f"=" * 60)
    print(f"Account:          {account_id}")
    print(f"Primary region:   {args.primary_region}")
    print(f"Secondary region: {args.secondary_region}")
    print(f"Primary bucket:   {primary_bucket}")
    print(f"Secondary bucket: {secondary_bucket}")
    print()

    if args.teardown:
        print("Teardown mode — removing resources...")
        teardown(args, account_id, primary_bucket, secondary_bucket)
        return

    s3_primary = boto3.client("s3", region_name=args.primary_region)
    s3_secondary = boto3.client("s3", region_name=args.secondary_region)
    iam = boto3.client("iam")

    # Step 1: Create buckets
    print("Step 1: Create S3 buckets")
    create_bucket(s3_primary, primary_bucket, args.primary_region)
    create_bucket(s3_secondary, secondary_bucket, args.secondary_region)

    # Step 2: Enable versioning
    print("\nStep 2: Enable versioning")
    enable_versioning(s3_primary, primary_bucket)
    enable_versioning(s3_secondary, secondary_bucket)

    # Step 3: Create IAM roles for replication
    print("\nStep 3: Create IAM replication roles")
    role_primary_to_secondary = create_replication_role(
        iam,
        f"s3-crr-{args.bucket_prefix}-primary-to-secondary",
        primary_bucket,
        secondary_bucket,
        account_id,
    )
    role_secondary_to_primary = create_replication_role(
        iam,
        f"s3-crr-{args.bucket_prefix}-secondary-to-primary",
        secondary_bucket,
        primary_bucket,
        account_id,
    )

    # Wait for IAM propagation
    print("\n  Waiting 10s for IAM propagation...")
    time.sleep(10)

    # Step 4: Set up bidirectional replication
    print("\nStep 4: Configure bidirectional CRR")
    setup_replication(s3_primary, primary_bucket, secondary_bucket, role_primary_to_secondary, account_id)
    setup_replication(s3_secondary, secondary_bucket, primary_bucket, role_secondary_to_primary, account_id)

    # Step 5: Summary
    print(f"\n{'=' * 60}")
    print("Setup complete! Configure your Lambda environment variables:\n")
    print(f"  # In {args.primary_region} Lambda:")
    print(f"  STATE_BACKEND=s3")
    print(f"  STATE_BUCKET={primary_bucket}")
    print(f"  STATE_PREFIX=failover-state/")
    print()
    print(f"  # In {args.secondary_region} Lambda:")
    print(f"  STATE_BACKEND=s3")
    print(f"  STATE_BUCKET={secondary_bucket}")
    print(f"  STATE_PREFIX=failover-state/")
    print()
    print("CRR replication typically takes 15-60 seconds for small objects.")
    print("The S3 backend uses ETag-based optimistic locking for consistency.")
    print()


def teardown(args, account_id, primary_bucket, secondary_bucket):
    """Remove all S3 CRR resources."""
    s3_primary = boto3.client("s3", region_name=args.primary_region)
    s3_secondary = boto3.client("s3", region_name=args.secondary_region)
    iam = boto3.client("iam")

    # Remove replication configs
    for s3_client, bucket in [(s3_primary, primary_bucket), (s3_secondary, secondary_bucket)]:
        try:
            s3_client.delete_bucket_replication(Bucket=bucket)
            print(f"  Removed replication config from {bucket}")
        except ClientError:
            pass

    # Empty and delete buckets
    for s3_client, bucket, region in [
        (s3_primary, primary_bucket, args.primary_region),
        (s3_secondary, secondary_bucket, args.secondary_region),
    ]:
        try:
            s3_res = boto3.resource("s3", region_name=region)
            bucket_obj = s3_res.Bucket(bucket)
            bucket_obj.object_versions.all().delete()
            bucket_obj.delete()
            print(f"  Deleted bucket {bucket}")
        except ClientError as e:
            print(f"  Could not delete {bucket}: {e}")

    # Remove IAM roles
    for role_name in [
        f"s3-crr-{args.bucket_prefix}-primary-to-secondary",
        f"s3-crr-{args.bucket_prefix}-secondary-to-primary",
    ]:
        try:
            # Delete inline policies first
            policies = iam.list_role_policies(RoleName=role_name)
            for policy_name in policies.get("PolicyNames", []):
                iam.delete_role_policy(RoleName=role_name, PolicyName=policy_name)
            iam.delete_role(RoleName=role_name)
            print(f"  Deleted IAM role {role_name}")
        except ClientError:
            pass

    print("\nTeardown complete.")


if __name__ == "__main__":
    main()
