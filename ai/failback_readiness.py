"""
AI-powered failback readiness assessment.

Analyzes stability trends (Aurora replication, ECS tasks, ALB errors) via LLM
to produce a GO/NO-GO/CAUTION verdict before failback. Non-blocking — if the
LLM call fails, returns CAUTION (proceed with warnings).
"""

import json
import logging
from typing import Optional

from ai.config import AI_RCA_PROVIDER, AI_RCA_MODEL
from ai.llm_client import call_llm

logger = logging.getLogger(__name__)


FAILBACK_READINESS_PROMPT = """\
You are an AWS infrastructure stability analyst for a multi-region failover system.

An operator is about to fail back traffic from the secondary region to the primary region. \
Before proceeding, you must assess whether the primary region is STABLE enough to receive traffic.

IMPORTANT: This is not a point-in-time health check — you must evaluate TRENDS over the observation window. \
A region that just came back online 2 minutes ago with fluctuating metrics is NOT ready. \
A region that has been stable for 15 minutes with consistent metrics IS ready.

## Stability Data

**Target Region:** {region}
**Observation Window:** Last {window_minutes} minutes

### Aurora Database
Replication Lag Trend:
{aurora_replication_lag}

Cluster Detail:
{aurora_cluster_detail}

Instance Status:
{aurora_instance_status}

Global Cluster Topology:
{aurora_global_topology}

Recent Aurora Events:
{aurora_events}

### ECS Service
Task Stability:
{ecs_task_stability}

### ALB Error Rate
{alb_error_trend}

## Instructions

Evaluate the stability of the target region and provide your assessment as a JSON block \
followed by your reasoning.

Output EXACTLY this format (JSON block first, then reasoning):

```json
{{
    "verdict": "GO" or "NO_GO" or "CAUTION",
    "confidence": <0-100>,
    "recommended_wait_minutes": <0 if GO, otherwise estimated minutes to wait>,
    "risks": ["risk 1", "risk 2"]
}}
```

REASONING:
<Your detailed analysis here. Explain what you observed in the trends and why you reached your verdict.>

Verdict guidelines:
- GO: All metrics are stable, no concerning trends, safe to proceed
- NO_GO: Active instability detected (rising error rates, replication lag spikes, task restarts)
- CAUTION: Mostly stable but minor concerns exist (proceed with close monitoring)

Be conservative — when in doubt, recommend CAUTION or NO_GO. Data integrity is paramount."""


def assess_failback_readiness(
    stability_context: dict, region: Optional[str] = None
) -> dict:
    """
    Assess failback readiness via LLM analysis of stability trends.

    Returns a structured dict with verdict, confidence, reasoning, and risks.
    Never raises — returns CAUTION verdict on any failure.
    """
    try:
        prompt = _build_failback_prompt(stability_context)
        logger.info(f"Calling LLM for failback readiness: provider={AI_RCA_PROVIDER}")
        llm_response = call_llm(prompt, region)

        if llm_response.startswith("[LLM]"):
            logger.warning(f"LLM call returned error: {llm_response}")
            return _fallback_result(llm_response)

        return _parse_readiness_response(llm_response)

    except Exception as e:
        logger.error(f"Failback readiness assessment failed: {type(e).__name__}: {e}")
        return _fallback_result(str(e))


def _build_failback_prompt(stability_context: dict) -> str:
    """Build the failback readiness prompt from stability data."""
    return FAILBACK_READINESS_PROMPT.format(
        region=stability_context.get("region", "unknown"),
        window_minutes=stability_context.get("window_minutes", "15"),
        aurora_replication_lag=json.dumps(
            stability_context.get("aurora_replication_lag", "N/A"), indent=2, default=str
        ),
        aurora_cluster_detail=json.dumps(
            stability_context.get("aurora_cluster_detail", "N/A"), indent=2, default=str
        ),
        aurora_instance_status=json.dumps(
            stability_context.get("aurora_instance_status", "N/A"), indent=2, default=str
        ),
        aurora_global_topology=json.dumps(
            stability_context.get("aurora_global_topology", "N/A"), indent=2, default=str
        ),
        aurora_events=json.dumps(
            stability_context.get("aurora_events", "N/A"), indent=2, default=str
        ),
        ecs_task_stability=json.dumps(
            stability_context.get("ecs_task_stability", "N/A"), indent=2, default=str
        ),
        alb_error_trend=json.dumps(
            stability_context.get("alb_error_trend", "N/A"), indent=2, default=str
        ),
    )


def _parse_readiness_response(llm_text: str) -> dict:
    """
    Parse the LLM response into a structured result.

    Extracts the JSON block from the response. Falls back to CAUTION if parsing fails.
    """
    result = {
        "verdict": "CAUTION",
        "confidence": 50,
        "reasoning": llm_text,
        "risks": [],
        "recommended_wait_minutes": 0,
        "raw_analysis": llm_text,
    }

    # Try to extract JSON block
    try:
        json_start = llm_text.find("{")
        json_end = llm_text.rfind("}") + 1
        if json_start >= 0 and json_end > json_start:
            json_str = llm_text[json_start:json_end]
            parsed = json.loads(json_str)

            if "verdict" in parsed:
                verdict = parsed["verdict"].upper()
                if verdict in ("GO", "NO_GO", "CAUTION"):
                    result["verdict"] = verdict
            if "confidence" in parsed:
                result["confidence"] = max(0, min(100, int(parsed["confidence"])))
            if "risks" in parsed:
                result["risks"] = parsed["risks"]
            if "recommended_wait_minutes" in parsed:
                result["recommended_wait_minutes"] = int(parsed["recommended_wait_minutes"])

            # Extract reasoning after the JSON block
            reasoning_text = llm_text[json_end:].strip()
            if reasoning_text.startswith("REASONING:"):
                reasoning_text = reasoning_text[len("REASONING:"):].strip()
            if reasoning_text:
                result["reasoning"] = reasoning_text

    except (json.JSONDecodeError, ValueError, TypeError) as e:
        logger.warning(f"Failed to parse LLM JSON response: {e}")
        # Keep the raw text as reasoning, verdict stays CAUTION

    return result


def _fallback_result(error_msg: str) -> dict:
    """Return a CAUTION result when the LLM is unavailable."""
    return {
        "verdict": "CAUTION",
        "confidence": 0,
        "reasoning": f"AI readiness assessment unavailable: {error_msg}. Proceed with manual verification.",
        "risks": ["AI assessment unavailable — manual checks recommended"],
        "recommended_wait_minutes": 0,
        "raw_analysis": error_msg,
    }


def format_readiness_for_sns(assessment: dict, stability_context: dict) -> str:
    """Format the readiness assessment for inclusion in an SNS notification."""
    separator = "-" * 60
    verdict = assessment.get("verdict", "CAUTION")
    confidence = assessment.get("confidence", 0)
    reasoning = assessment.get("reasoning", "")
    risks = assessment.get("risks", [])
    wait = assessment.get("recommended_wait_minutes", 0)

    verdict_display = {
        "GO": "GO — Safe to proceed",
        "NO_GO": "NO GO — Do NOT proceed",
        "CAUTION": "CAUTION — Proceed with monitoring",
    }.get(verdict, verdict)

    provider_label = f"{AI_RCA_PROVIDER.capitalize()}/{AI_RCA_MODEL}"

    lines = [
        f"\n{separator}",
        "AI FAILBACK READINESS ASSESSMENT",
        separator,
        "",
        f"Verdict:    {verdict_display}",
        f"Confidence: {confidence}%",
    ]

    if wait > 0:
        lines.append(f"Wait:       Recommend waiting {wait} more minutes")

    if risks:
        lines.append("")
        lines.append("Risks:")
        for risk in risks:
            lines.append(f"  - {risk}")

    lines.extend([
        "",
        "Analysis:",
        reasoning,
        "",
        separator,
        f"Model: {provider_label} | "
        f"Region: {stability_context.get('region', 'unknown')} | "
        f"Window: {stability_context.get('window_minutes', '?')}m",
        "This is an AI-generated assessment. Verify before acting.",
    ])

    return "\n".join(lines)
