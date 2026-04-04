"""
Shared LLM client for Claude and Gemini API calls.

Used by rca_analyzer, failback_readiness, and aurora_advisor modules.
No SDK dependencies — uses urllib for HTTP calls.
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
    AI_RCA_PROVIDER,
    AI_RCA_TIMEOUT_SECONDS,
    ANTHROPIC_API_KEY_SECRET_NAME,
    GEMINI_API_KEY_SECRET_NAME,
)

logger = logging.getLogger(__name__)


def _get_secret(secret_name: str, region: Optional[str] = None) -> str:
    """Retrieve a secret from Secrets Manager."""
    try:
        sm = boto3.client(
            "secretsmanager",
            region_name=region or os.environ.get("AWS_REGION", "us-east-1"),
        )
        resp = sm.get_secret_value(SecretId=secret_name)
        return resp["SecretString"]
    except ClientError as e:
        logger.error(f"Failed to retrieve secret {secret_name}: {e}")
        raise


def get_api_key(region: Optional[str] = None) -> str:
    """Retrieve API key for the configured provider."""
    if AI_RCA_PROVIDER == "gemini":
        key = os.environ.get("GEMINI_API_KEY")
        if key:
            return key
        return _get_secret(GEMINI_API_KEY_SECRET_NAME, region)
    else:
        key = os.environ.get("ANTHROPIC_API_KEY")
        if key:
            return key
        return _get_secret(ANTHROPIC_API_KEY_SECRET_NAME, region)


def _call_claude(api_key: str, prompt: str, max_tokens: int = None,
                 timeout: int = None) -> str:
    """Call the Claude API and return the response text."""
    request_body = json.dumps({
        "model": AI_RCA_MODEL,
        "max_tokens": max_tokens or AI_RCA_MAX_TOKENS,
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

    with urllib.request.urlopen(req, timeout=timeout or AI_RCA_TIMEOUT_SECONDS) as resp:
        result = json.loads(resp.read().decode("utf-8"))

    content_blocks = result.get("content", [])
    analysis = "\n".join(
        block["text"] for block in content_blocks if block.get("type") == "text"
    )

    if not analysis:
        return "[LLM] Claude returned empty response"
    return analysis


def _call_gemini(api_key: str, prompt: str, max_tokens: int = None,
                 timeout: int = None) -> str:
    """Call the Gemini API and return the response text."""
    url = (
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"{AI_RCA_MODEL}:generateContent?key={api_key}"
    )

    request_body = json.dumps({
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "maxOutputTokens": max_tokens or AI_RCA_MAX_TOKENS,
        },
    })

    req = urllib.request.Request(
        url,
        data=request_body.encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=timeout or AI_RCA_TIMEOUT_SECONDS) as resp:
        result = json.loads(resp.read().decode("utf-8"))

    candidates = result.get("candidates", [])
    if not candidates:
        return "[LLM] Gemini returned empty response"

    parts = candidates[0].get("content", {}).get("parts", [])
    analysis = "\n".join(part["text"] for part in parts if "text" in part)

    if not analysis:
        return "[LLM] Gemini returned empty response"
    return analysis


def call_llm(prompt: str, region: Optional[str] = None,
             max_tokens: int = None, timeout: int = None) -> str:
    """
    Call the configured LLM provider and return the response text.

    This is the unified entry point for all AI modules. Never raises —
    returns an error string on failure so callers can remain non-blocking.
    """
    try:
        api_key = get_api_key(region)

        logger.info(f"Calling LLM: provider={AI_RCA_PROVIDER}, model={AI_RCA_MODEL}")

        if AI_RCA_PROVIDER == "gemini":
            return _call_gemini(api_key, prompt, max_tokens, timeout)
        else:
            return _call_claude(api_key, prompt, max_tokens, timeout)

    except Exception as e:
        logger.error(f"LLM call failed: {type(e).__name__}: {e}")
        return f"[LLM] Call failed: {type(e).__name__}: {e}"
