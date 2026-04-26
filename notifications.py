"""Shared notification template for Vigil Lambdas (v1.6).

Provides ``compose_message()`` — a single contract used by the failover
orchestrator and the manual failback Lambda for every operator-facing
notification. The goal: an operator who has never seen this system can
read any one email and immediately understand:

  * **WHAT** is happening (one-line summary)
  * **WHY** it is happening (root signal evidence in plain English)
  * **WHAT TO DO NEXT** (explicit operator action — including
    "no action required, the system will retry")

Optional sections:

  * **journey** — 3-line breadcrumb showing where in the failover lifecycle
    this notification falls. Lets operators triage quickly even if they
    missed earlier emails.
  * **context** — a small key/value table of timestamps, regions, counters.

The composed (subject, body) tuple does NOT include the
``[ENVIRONMENT-APP_NAME]`` prefix; each Lambda's ``_format_subject()`` adds
that. Keeping prefix logic Lambda-side avoids duplicating env-var reads in
this module and keeps the helper purely formatting-focused.

Used by ``failover_orchestrator_v3.py`` and ``manual_failback_v2.py``.
Both Lambda zips must include this file (see CLAUDE.md deploy commands).
"""

from datetime import datetime, timezone
from typing import Optional


SEVERITY_INFO = "INFO"
SEVERITY_WARNING = "WARNING"
SEVERITY_CRITICAL = "CRITICAL"

_VALID_SEVERITIES = frozenset({SEVERITY_INFO, SEVERITY_WARNING, SEVERITY_CRITICAL})


def compose_message(
    *,
    severity: str,
    what: str,
    why: str,
    next_step: str,
    context: Optional[dict] = None,
    journey: Optional[list] = None,
    source: str = "",
    region: str = "",
    version: str = "v1.6",
    now: Optional[datetime] = None,
) -> tuple:
    """Build a structured (subject, body) pair for an SNS notification.

    All four content fields (severity, what, why, next_step) are required.
    Empty strings are rejected so call sites cannot silently ship a
    notification with a missing operator instruction — that is exactly the
    "buggy journey" the v1.5 drill flagged.

    Args:
        severity: One of ``INFO``, ``WARNING``, ``CRITICAL``. Becomes the
            leading token of the subject (after the bracketed env-app prefix).
        what: One-line summary, e.g.
            "Region us-west-1 reported its FIRST health failure (1 of 3)".
        why: One to three plain-English sentences explaining the root signal.
            Avoid raw JSON dumps — paste health-signal blobs into ``context``
            instead so the body stays readable in an email client.
        next_step: Explicit operator action. When no human action is needed
            yet, say so: "No action — the system will retry. You will be
            notified again if the failure persists."
        context: Optional ``{label: value}`` dict rendered as a small aligned
            table under a CONTEXT heading. Use for timestamps, regions,
            counters — anything tabular.
        journey: Optional list of breadcrumb strings (typically 3) showing
            position in the incident lifecycle, e.g.
            ``["[1] First failure", "[ ] Sustained", "[ ] Failover"]``.
        source: Lambda function name shown in the footer (e.g.,
            ``"failover-orchestrator"``). Empty string omits it.
        region: Publishing region shown in the footer (e.g., ``"us-west-1"``).
            Empty string omits it.
        version: Vigil version string shown in the footer.
        now: Override timestamp for tests; production should leave unset.

    Returns:
        ``(subject, body)``. The subject is the bare ``"<SEVERITY>: <what>"``
        form; each Lambda's ``_format_subject()`` prepends the
        ``[ENVIRONMENT-APP_NAME]`` prefix and applies the 100-char SNS truncation.

    Raises:
        ValueError: If ``severity`` is not one of the allowed values, or if
            any of ``what``, ``why``, ``next_step`` is empty.
    """
    if severity not in _VALID_SEVERITIES:
        raise ValueError(
            f"severity must be one of {sorted(_VALID_SEVERITIES)}, got {severity!r}"
        )
    if not what:
        raise ValueError("compose_message: 'what' is required and cannot be empty")
    if not why:
        raise ValueError("compose_message: 'why' is required and cannot be empty")
    if not next_step:
        raise ValueError("compose_message: 'next_step' is required and cannot be empty")

    subject = f"{severity}: {what}"

    sections = ["WHAT IS HAPPENING", why]

    if journey:
        sections.append("")
        sections.append("WHERE WE ARE IN THE INCIDENT")
        sections.extend(str(line) for line in journey)

    if context:
        sections.append("")
        sections.append("CONTEXT")
        max_label = max(len(str(k)) for k in context)
        for label, value in context.items():
            sections.append(f"  {str(label).ljust(max_label)} : {value}")

    sections.append("")
    sections.append("WHAT TO DO NEXT")
    sections.append(next_step)

    sections.append("")
    sections.append("—")

    ts = (now or datetime.now(timezone.utc)).isoformat(timespec="seconds")
    footer = [f"Vigil {version}", ts]
    if source:
        footer.append(source)
    if region:
        footer.append(f"region={region}")
    sections.append(" · ".join(footer))

    return subject, "\n".join(sections)
