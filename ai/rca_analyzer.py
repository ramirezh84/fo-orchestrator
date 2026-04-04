"""
Root Cause Analysis using Claude or Gemini API.

Accepts collected incident context and returns a structured RCA summary
suitable for SNS notification to operators.
"""

import json
import logging
from typing import Optional

from ai.config import AI_RCA_MODEL, AI_RCA_PROVIDER
from ai.llm_client import (  # noqa: F401 — re-exported for backward compat
    call_llm,
    get_api_key,
    _call_claude,
    _call_gemini,
    _get_secret,
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


def _build_prompt(incident_context: dict) -> str:
    """Build the RCA prompt from incident context."""
    return RCA_PROMPT_TEMPLATE.format(
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


def analyze_incident(incident_context: dict, region: Optional[str] = None) -> str:
    """
    Send incident context to the configured LLM provider and return RCA summary.

    Returns the analysis text, or an error message if the API call fails.
    This function must never raise — RCA failure should not block failover.
    """
    try:
        prompt = _build_prompt(incident_context)
        logger.info(f"Using provider: {AI_RCA_PROVIDER}, model: {AI_RCA_MODEL}")
        return call_llm(prompt, region)

    except Exception as e:
        logger.error(f"RCA analysis failed: {type(e).__name__}: {e}")
        return f"[RCA] Analysis unavailable: {type(e).__name__}: {e}"


def format_rca_for_sns(rca_text: str, incident_context: dict) -> str:
    """Format the RCA analysis for inclusion in an SNS notification."""
    separator = "-" * 60
    provider_label = f"{AI_RCA_PROVIDER.capitalize()}/{AI_RCA_MODEL}"
    return (
        f"\n{separator}\n"
        f"AI ROOT CAUSE ANALYSIS\n"
        f"{separator}\n\n"
        f"{rca_text}\n\n"
        f"{separator}\n"
        f"Model: {provider_label} | "
        f"Region: {incident_context.get('region', 'unknown')} | "
        f"Log window: {incident_context.get('window_minutes', '?')}m\n"
        f"This is an AI-generated analysis. Verify before acting.\n"
    )
