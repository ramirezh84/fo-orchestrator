"""
Root Cause Analysis using Claude API.

Accepts collected incident context and returns a structured RCA summary
suitable for SNS notification to operators.
"""

import json
import logging
import os
import urllib.request
from typing import Optional

import boto3
from botocore.exceptions import ClientError

from ai.config import (
    AI_RCA_MAX_TOKENS,
    AI_RCA_MODEL,
    AI_RCA_TIMEOUT_SECONDS,
    ANTHROPIC_API_KEY_SECRET_NAME,
)

logger = logging.getLogger(__name__)

# Prompt template for RCA analysis
RCA_PROMPT_TEMPLATE = """\
You are an AWS infrastructure incident analyst for a multi-region failover system.

A failover has just been triggered. Analyze the incident context below and produce a concise root cause analysis.

## Incident Context

**Region:** {region}
**Timestamp:** {timestamp}
**Window:** Last {window_minutes} minutes

### Health Signals (from orchestrator evaluation)
{health_signals}

### ECS Service State
{ecs_events}

### Aurora Database Status
{aurora_status}

### ALB Target Health
{alb_health}

### Application Logs (errors/warnings)
{application_logs}

## Instructions

Produce a structured analysis with these sections:

1. **Timeline** — Key events in chronological order leading to the failover
2. **Root Cause** — Most likely root cause based on the evidence (be specific)
3. **Affected Components** — Which health signals failed and why
4. **Impact** — What was the user-facing impact
5. **Recommended Actions** — Immediate steps the operator should take (beyond the automated failover)

Be concise and actionable. Operators are reading this during an incident.
Do NOT add a title or heading — start directly with the Timeline section.
Use plain text only — no markdown formatting (no **, no #, no tables). Use CAPS for emphasis instead."""


def get_api_key(region: Optional[str] = None) -> str:
    """Retrieve Anthropic API key from Secrets Manager."""
    # Allow direct env var override for testing
    key = os.environ.get("ANTHROPIC_API_KEY")
    if key:
        return key

    try:
        sm = boto3.client(
            "secretsmanager",
            region_name=region or os.environ.get("AWS_REGION", "us-east-1"),
        )
        resp = sm.get_secret_value(SecretId=ANTHROPIC_API_KEY_SECRET_NAME)
        return resp["SecretString"]
    except ClientError as e:
        logger.error(f"Failed to retrieve API key from Secrets Manager: {e}")
        raise


def analyze_incident(incident_context: dict, region: Optional[str] = None) -> str:
    """
    Send incident context to Claude API and return RCA summary.

    Returns the analysis text, or an error message if the API call fails.
    This function must never raise — RCA failure should not block failover.
    """
    try:
        api_key = get_api_key(region)

        prompt = RCA_PROMPT_TEMPLATE.format(
            region=incident_context.get("region", "unknown"),
            timestamp=incident_context.get("timestamp", "unknown"),
            window_minutes=incident_context.get("window_minutes", "10"),
            health_signals=json.dumps(
                incident_context.get("health_signals", {}), indent=2, default=str
            ),
            ecs_events=json.dumps(
                incident_context.get("ecs_events", {}), indent=2, default=str
            ),
            aurora_status=json.dumps(
                incident_context.get("aurora_status", {}), indent=2, default=str
            ),
            alb_health=json.dumps(
                incident_context.get("alb_health", "N/A"), indent=2, default=str
            ),
            application_logs=json.dumps(
                incident_context.get("application_logs", "N/A"), indent=2, default=str
            ),
        )

        # Use raw HTTP via urllib to avoid adding anthropic SDK as a Lambda dependency
        request_body = json.dumps({
            "model": AI_RCA_MODEL,
            "max_tokens": AI_RCA_MAX_TOKENS,
            "messages": [{"role": "user", "content": prompt}],
        })

        req = urllib.request.Request(
            "https://api.anthropic.com/v1/messages",
            data=request_body.encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
            },
            method="POST",
        )

        with urllib.request.urlopen(req, timeout=AI_RCA_TIMEOUT_SECONDS) as resp:
            result = json.loads(resp.read().decode("utf-8"))

        # Extract text from response
        content_blocks = result.get("content", [])
        analysis = "\n".join(
            block["text"] for block in content_blocks if block.get("type") == "text"
        )

        if not analysis:
            return "[RCA] Claude returned empty response"

        return analysis

    except Exception as e:
        logger.error(f"RCA analysis failed: {type(e).__name__}: {e}")
        return f"[RCA] Analysis unavailable: {type(e).__name__}: {e}"


def format_rca_for_sns(rca_text: str, incident_context: dict) -> str:
    """Format the RCA analysis for inclusion in an SNS notification."""
    separator = "-" * 60
    return (
        f"\n{separator}\n"
        f"AI ROOT CAUSE ANALYSIS\n"
        f"{separator}\n\n"
        f"{rca_text}\n\n"
        f"{separator}\n"
        f"Model: {AI_RCA_MODEL} | "
        f"Region: {incident_context.get('region', 'unknown')} | "
        f"Log window: {incident_context.get('window_minutes', '?')}m\n"
        f"This is an AI-generated analysis. Verify before acting.\n"
    )
