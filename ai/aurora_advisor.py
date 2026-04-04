"""
AI-powered Aurora promotion advisor.

Progressive automation for Aurora Global Database promotion decisions:
- advisory:   LLM recommends, operator decides (SNS notification only)
- guided:     Auto-execute if LLM confidence >= threshold AND switchover
- autonomous: Auto-execute with hard guardrails (deterministic safety checks)

Non-blocking — if the LLM call fails, falls back to manual operation.
"""

import json
import logging
from typing import Optional

from ai.config import (
    AI_AURORA_ADVISOR_CONFIDENCE_THRESHOLD,
    AI_AURORA_ADVISOR_MAX_LAG_MS,
    AI_AURORA_ADVISOR_MODE,
    AI_RCA_MODEL,
    AI_RCA_PROVIDER,
)
from ai.llm_client import call_llm

logger = logging.getLogger(__name__)


AURORA_ADVISOR_PROMPT = """\
You are an AWS Aurora Global Database promotion specialist for a multi-region failover system.

A failover has been triggered and Aurora needs to be promoted in the target region. \
You must analyze the current Aurora state and recommend the safest promotion method.

CRITICAL: Data integrity is paramount. A premature promotion can cause data loss. \
When in doubt, recommend switchover (no data loss) or manual intervention.

## Failover Scenario
**Type:** {scenario}
- app_failure: Primary region app is down but the region itself is reachable. \
Planned switchover (zero data loss) may be possible.
- region_failure: Primary region is unreachable. Only unplanned failover \
(with potential data loss) is available.

## Aurora Stability Data

**Target Region:** {region}
**Observation Window:** Last {window_minutes} minutes

### Replication Lag Trend
{aurora_replication_lag}

### Cluster Detail (target region)
{aurora_cluster_detail}

### Instance Status
{aurora_instance_status}

### Global Cluster Topology
{aurora_global_topology}

### Recent Aurora Events
{aurora_events}

## Instructions

Analyze the Aurora state and recommend a promotion method. Output EXACTLY this format:

```json
{{
    "recommended_method": "switchover" or "failover",
    "confidence": <0-100>,
    "data_loss_risk": "none" or "low" or "medium" or "high",
    "estimated_data_loss_ms": <0 for switchover, estimated ms for failover>,
    "warnings": ["warning 1", "warning 2"]
}}
```

REASONING:
<Your detailed analysis. Explain replication lag trends, cluster health, and why you chose this method.>

Method guidelines:
- switchover (SwitchoverGlobalCluster): Zero data loss, requires primary to be reachable. \
Recommend when scenario is app_failure AND replication lag is low and stable.
- failover (FailoverGlobalCluster --allow-data-loss): May lose recent writes. \
Required when scenario is region_failure. May also be needed if switchover is not viable.

Confidence guidelines:
- 90-100: Clear recommendation, all indicators align
- 70-89: Recommendation is sound but minor concerns exist
- 50-69: Significant uncertainty, manual review recommended
- <50: Do not auto-execute under any circumstances"""


def advise_aurora_promotion(
    stability_context: dict,
    scenario: str,
    mode: str = None,
    region: Optional[str] = None,
) -> dict:
    """
    Advise on Aurora promotion method and whether to auto-execute.

    Args:
        stability_context: Output from collect_stability_context()
        scenario: "app_failure" or "region_failure"
        mode: Override for AI_AURORA_ADVISOR_MODE (advisory/guided/autonomous)
        region: AWS region for Secrets Manager

    Returns a structured dict with recommendation and execution decision.
    Never raises — returns a safe fallback on any failure.
    """
    effective_mode = mode or AI_AURORA_ADVISOR_MODE

    if effective_mode == "disabled":
        return _disabled_result()

    # Phase 3: Run hard guardrails BEFORE LLM call (deterministic, fast)
    guardrail_result = _apply_hard_guardrails(stability_context, scenario)

    try:
        prompt = _build_aurora_advisor_prompt(stability_context, scenario)
        logger.info(f"Calling LLM for Aurora advisor: provider={AI_RCA_PROVIDER}, mode={effective_mode}")
        llm_response = call_llm(prompt, region)

        if llm_response.startswith("[LLM]"):
            logger.warning(f"LLM call returned error: {llm_response}")
            return _fallback_result(llm_response, scenario)

        recommendation = _parse_advisor_response(llm_response, scenario)

    except Exception as e:
        logger.error(f"Aurora advisor failed: {type(e).__name__}: {e}")
        return _fallback_result(str(e), scenario)

    # Apply guardrail override (deterministic checks beat LLM)
    if not guardrail_result["passed"]:
        recommendation["guardrails_passed"] = False
        recommendation["guardrail_reasons"] = guardrail_result["reasons"]
        recommendation["should_auto_execute"] = False
        return recommendation

    recommendation["guardrails_passed"] = True
    recommendation["guardrail_reasons"] = []

    # Determine auto-execute based on mode
    recommendation["should_auto_execute"] = _should_auto_execute(
        recommendation, effective_mode
    )

    return recommendation


def _build_aurora_advisor_prompt(stability_context: dict, scenario: str) -> str:
    """Build the Aurora advisor prompt from stability data."""
    return AURORA_ADVISOR_PROMPT.format(
        scenario=scenario,
        region=stability_context.get("region", "unknown"),
        window_minutes=stability_context.get("window_minutes", "10"),
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
    )


def _parse_advisor_response(llm_text: str, scenario: str) -> dict:
    """Parse the LLM response into a structured recommendation."""
    # Default: safe fallback
    result = {
        "recommended_method": "failover" if scenario == "region_failure" else "switchover",
        "confidence": 50,
        "data_loss_risk": "medium",
        "estimated_data_loss_ms": 0,
        "warnings": [],
        "reasoning": llm_text,
        "raw_analysis": llm_text,
        "should_auto_execute": False,
        "guardrails_passed": True,
        "guardrail_reasons": [],
    }

    try:
        json_start = llm_text.find("{")
        json_end = llm_text.rfind("}") + 1
        if json_start >= 0 and json_end > json_start:
            json_str = llm_text[json_start:json_end]
            parsed = json.loads(json_str)

            if "recommended_method" in parsed:
                method = parsed["recommended_method"].lower()
                if method in ("switchover", "failover"):
                    result["recommended_method"] = method
            if "confidence" in parsed:
                result["confidence"] = max(0, min(100, int(parsed["confidence"])))
            if "data_loss_risk" in parsed:
                risk = parsed["data_loss_risk"].lower()
                if risk in ("none", "low", "medium", "high"):
                    result["data_loss_risk"] = risk
            if "estimated_data_loss_ms" in parsed:
                result["estimated_data_loss_ms"] = max(0, int(parsed["estimated_data_loss_ms"]))
            if "warnings" in parsed:
                result["warnings"] = parsed["warnings"]

            # Extract reasoning after JSON block
            reasoning_text = llm_text[json_end:].strip()
            if reasoning_text.startswith("REASONING:"):
                reasoning_text = reasoning_text[len("REASONING:"):].strip()
            if reasoning_text:
                result["reasoning"] = reasoning_text

    except (json.JSONDecodeError, ValueError, TypeError) as e:
        logger.warning(f"Failed to parse Aurora advisor JSON: {e}")

    return result


# ── Hard Guardrails (Phase 3) ─────────────────────────────────────────────────


def _apply_hard_guardrails(stability_context: dict, scenario: str) -> dict:
    """
    Deterministic safety checks that run BEFORE the LLM call.

    These CANNOT be overridden by the LLM. If any guardrail fails,
    auto-execution is blocked regardless of LLM confidence.

    Returns: {"passed": bool, "reasons": [str]}
    """
    reasons = []

    # Guardrail 1: Replication lag must be below threshold
    lag_data = stability_context.get("aurora_replication_lag", {})
    if "replicas" in lag_data:
        for replica_id, series in lag_data["replicas"].items():
            summary = series.get("summary")
            if summary and summary.get("max") is not None:
                if summary["max"] > AI_AURORA_ADVISOR_MAX_LAG_MS:
                    reasons.append(
                        f"Replication lag on {replica_id} exceeded {AI_AURORA_ADVISOR_MAX_LAG_MS}ms "
                        f"(max={summary['max']}ms in window)"
                    )
            # Check last 3 datapoints if available
            datapoints = series.get("datapoints", [])
            recent = datapoints[-3:] if len(datapoints) >= 3 else datapoints
            high_recent = [dp for dp in recent if dp.get("value", 0) > AI_AURORA_ADVISOR_MAX_LAG_MS]
            if len(high_recent) == len(recent) and len(recent) >= 3:
                reasons.append(
                    f"Replication lag on {replica_id} consistently above "
                    f"{AI_AURORA_ADVISOR_MAX_LAG_MS}ms for last {len(recent)} datapoints"
                )

    # Guardrail 2: Global cluster synchronization status must be "connected"
    topology = stability_context.get("aurora_global_topology", {})
    for member in topology.get("members", []):
        sync_status = member.get("synchronization_status", "unknown")
        if sync_status not in ("connected", "unknown"):
            cluster_arn = member.get("cluster_arn", "unknown")
            reasons.append(
                f"Global cluster member {cluster_arn} sync status is "
                f"'{sync_status}' (expected 'connected')"
            )

    # Guardrail 3: Cluster status must be "available"
    cluster_detail = stability_context.get("aurora_cluster_detail", {})
    cluster_status = cluster_detail.get("status")
    if cluster_status and cluster_status not in ("available", "backing-up"):
        reasons.append(f"Cluster status is '{cluster_status}' (expected 'available')")

    # Guardrail 4: No instances in transitional states
    instance_data = stability_context.get("aurora_instance_status", {})
    bad_states = {"modifying", "rebooting", "failing-over", "maintenance", "upgrading"}
    for inst in instance_data.get("instances", []):
        inst_status = inst.get("status", "")
        if inst_status in bad_states:
            reasons.append(
                f"Instance {inst.get('instance_id')} is in '{inst_status}' state"
            )

    return {"passed": len(reasons) == 0, "reasons": reasons}


# ── Auto-Execute Decision ─────────────────────────────────────────────────────


def _should_auto_execute(recommendation: dict, mode: str) -> bool:
    """
    Determine whether to auto-execute based on mode and recommendation.

    - advisory:   Never auto-execute
    - guided:     Only if confidence >= threshold AND method is switchover
    - autonomous: Always (guardrails already checked upstream)
    """
    if mode == "advisory":
        return False

    confidence = recommendation.get("confidence", 0)
    method = recommendation.get("recommended_method", "")

    if mode == "guided":
        return (
            confidence >= AI_AURORA_ADVISOR_CONFIDENCE_THRESHOLD
            and method == "switchover"
        )

    if mode == "autonomous":
        return True

    return False


# ── Fallback Results ───────��──────────────────────────────────────────────────


def _disabled_result() -> dict:
    """Return when advisor is disabled."""
    return {
        "recommended_method": "",
        "confidence": 0,
        "data_loss_risk": "",
        "estimated_data_loss_ms": 0,
        "warnings": [],
        "reasoning": "",
        "raw_analysis": "",
        "should_auto_execute": False,
        "guardrails_passed": True,
        "guardrail_reasons": [],
    }


def _fallback_result(error_msg: str, scenario: str) -> dict:
    """Return when the LLM is unavailable — defer to operator."""
    return {
        "recommended_method": "failover" if scenario == "region_failure" else "switchover",
        "confidence": 0,
        "data_loss_risk": "unknown",
        "estimated_data_loss_ms": 0,
        "warnings": [f"AI advisor unavailable: {error_msg}"],
        "reasoning": f"Aurora advisor unavailable: {error_msg}. Manual intervention required.",
        "raw_analysis": error_msg,
        "should_auto_execute": False,
        "guardrails_passed": True,
        "guardrail_reasons": [],
    }


# ── SNS Formatting ─────────────────────────────────────��──────────────────────


def format_advisor_for_sns(recommendation: dict, stability_context: dict) -> str:
    """Format the Aurora advisor recommendation for SNS notification."""
    separator = "-" * 60
    method = recommendation.get("recommended_method", "unknown")
    confidence = recommendation.get("confidence", 0)
    risk = recommendation.get("data_loss_risk", "unknown")
    estimated_loss = recommendation.get("estimated_data_loss_ms", 0)
    reasoning = recommendation.get("reasoning", "")
    warnings = recommendation.get("warnings", [])
    auto_exec = recommendation.get("should_auto_execute", False)
    guardrails_passed = recommendation.get("guardrails_passed", True)
    guardrail_reasons = recommendation.get("guardrail_reasons", [])

    method_display = {
        "switchover": "SWITCHOVER (planned, zero data loss)",
        "failover": "FAILOVER (unplanned, potential data loss)",
    }.get(method, method)

    provider_label = f"{AI_RCA_PROVIDER.capitalize()}/{AI_RCA_MODEL}"

    lines = [
        f"\n{separator}",
        "AI AURORA PROMOTION ADVISOR",
        separator,
        "",
        f"Recommended Method: {method_display}",
        f"Confidence:         {confidence}%",
        f"Data Loss Risk:     {risk}",
    ]

    if estimated_loss > 0:
        lines.append(f"Estimated Loss:     ~{estimated_loss}ms of transactions")

    if auto_exec:
        lines.append("")
        lines.append("ACTION: Auto-executing promotion based on advisor recommendation.")
    else:
        lines.append("")
        lines.append("ACTION: Manual promotion required. Review recommendation above.")

    if not guardrails_passed:
        lines.append("")
        lines.append("GUARDRAILS BLOCKED AUTO-EXECUTION:")
        for reason in guardrail_reasons:
            lines.append(f"  ! {reason}")

    if warnings:
        lines.append("")
        lines.append("Warnings:")
        for w in warnings:
            lines.append(f"  - {w}")

    lines.extend([
        "",
        "Analysis:",
        reasoning,
        "",
        separator,
        f"Model: {provider_label} | "
        f"Mode: {AI_AURORA_ADVISOR_MODE} | "
        f"Region: {stability_context.get('region', 'unknown')} | "
        f"Window: {stability_context.get('window_minutes', '?')}m",
        "This is an AI-generated recommendation. Verify before acting.",
    ])

    return "\n".join(lines)
