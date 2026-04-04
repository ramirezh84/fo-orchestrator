#!/usr/bin/env python3
"""
Tests for the shared LLM client module.

Run: python3 -m pytest tests/test_llm_client.py -v
"""

import json
import os
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

os.environ.setdefault("SNS_TOPIC_ARN", "arn:aws:sns:us-east-1:123456789:test")
os.environ.setdefault("AI_RCA_ENABLED", "false")


class TestGetApiKey:
    """Tests for API key retrieval."""

    @patch("ai.llm_client.os.environ", {"ANTHROPIC_API_KEY": "test-key"})
    def test_claude_key_from_env(self):
        from ai.llm_client import get_api_key
        assert get_api_key() == "test-key"

    @patch("ai.llm_client.os.environ", {"GEMINI_API_KEY": "gem-key"})
    @patch("ai.llm_client.AI_RCA_PROVIDER", "gemini")
    def test_gemini_key_from_env(self):
        from ai.llm_client import get_api_key
        assert get_api_key() == "gem-key"

    @patch("ai.llm_client.os.environ", {})
    @patch("ai.llm_client.boto3")
    def test_key_from_secrets_manager(self, mock_boto3):
        from ai.llm_client import get_api_key

        mock_sm = MagicMock()
        mock_boto3.client.return_value = mock_sm
        mock_sm.get_secret_value.return_value = {"SecretString": "sk-ant-secret"}

        key = get_api_key("us-east-1")
        assert key == "sk-ant-secret"
        mock_sm.get_secret_value.assert_called_once()

    @patch("ai.llm_client.os.environ", {})
    @patch("ai.llm_client.boto3")
    def test_secrets_manager_failure_raises(self, mock_boto3):
        from ai.llm_client import get_api_key

        mock_sm = MagicMock()
        mock_boto3.client.return_value = mock_sm
        mock_sm.get_secret_value.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}},
            "GetSecretValue",
        )

        with pytest.raises(ClientError):
            get_api_key("us-east-1")


class TestCallClaude:
    """Tests for Claude API calls."""

    @patch("ai.llm_client.urllib.request.urlopen")
    def test_success(self, mock_urlopen):
        from ai.llm_client import _call_claude

        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({
            "content": [{"type": "text", "text": "Analysis result"}]
        }).encode()
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        result = _call_claude("test-key", "test prompt")
        assert result == "Analysis result"

    @patch("ai.llm_client.urllib.request.urlopen")
    def test_empty_response(self, mock_urlopen):
        from ai.llm_client import _call_claude

        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({"content": []}).encode()
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        result = _call_claude("test-key", "test prompt")
        assert "empty response" in result

    @patch("ai.llm_client.urllib.request.urlopen")
    def test_sends_correct_request(self, mock_urlopen):
        from ai.llm_client import _call_claude

        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({
            "content": [{"type": "text", "text": "ok"}]
        }).encode()
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        _call_claude("test-key", "my prompt", max_tokens=1000, timeout=10)

        req = mock_urlopen.call_args[0][0]
        assert req.full_url == "https://api.anthropic.com/v1/messages"
        assert req.get_header("X-api-key") == "test-key"
        assert req.get_header("Anthropic-version") == "2023-06-01"

        body = json.loads(req.data.decode())
        assert body["max_tokens"] == 1000
        assert body["messages"][0]["content"] == "my prompt"

        assert mock_urlopen.call_args[1]["timeout"] == 10


class TestCallGemini:
    """Tests for Gemini API calls."""

    @patch("ai.llm_client.urllib.request.urlopen")
    def test_success(self, mock_urlopen):
        from ai.llm_client import _call_gemini

        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({
            "candidates": [{"content": {"parts": [{"text": "Gemini result"}]}}]
        }).encode()
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        result = _call_gemini("gem-key", "test prompt")
        assert result == "Gemini result"

    @patch("ai.llm_client.urllib.request.urlopen")
    def test_empty_candidates(self, mock_urlopen):
        from ai.llm_client import _call_gemini

        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({"candidates": []}).encode()
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        result = _call_gemini("gem-key", "test prompt")
        assert "empty response" in result

    @patch("ai.llm_client.AI_RCA_MODEL", "gemini-2.5-flash")
    @patch("ai.llm_client.urllib.request.urlopen")
    def test_sends_correct_request(self, mock_urlopen):
        from ai.llm_client import _call_gemini

        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({
            "candidates": [{"content": {"parts": [{"text": "ok"}]}}]
        }).encode()
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        _call_gemini("gem-key", "my prompt")

        req = mock_urlopen.call_args[0][0]
        assert "generativelanguage.googleapis.com" in req.full_url
        assert "key=gem-key" in req.full_url

        body = json.loads(req.data.decode())
        assert "contents" in body
        assert body["contents"][0]["parts"][0]["text"] == "my prompt"


class TestCallLLM:
    """Tests for the unified call_llm entry point."""

    @patch("ai.llm_client.AI_RCA_PROVIDER", "claude")
    @patch("ai.llm_client._call_claude", return_value="claude result")
    @patch("ai.llm_client.get_api_key", return_value="key")
    def test_dispatches_to_claude(self, mock_key, mock_claude):
        from ai.llm_client import call_llm

        result = call_llm("prompt")
        assert result == "claude result"
        mock_claude.assert_called_once()

    @patch("ai.llm_client.AI_RCA_PROVIDER", "gemini")
    @patch("ai.llm_client._call_gemini", return_value="gemini result")
    @patch("ai.llm_client.get_api_key", return_value="key")
    def test_dispatches_to_gemini(self, mock_key, mock_gemini):
        from ai.llm_client import call_llm

        result = call_llm("prompt")
        assert result == "gemini result"
        mock_gemini.assert_called_once()

    @patch("ai.llm_client.get_api_key", side_effect=Exception("auth failed"))
    def test_never_raises(self, mock_key):
        from ai.llm_client import call_llm

        result = call_llm("prompt")
        assert "[LLM] Call failed" in result
        assert "auth failed" in result

    @patch("ai.llm_client.urllib.request.urlopen")
    @patch("ai.llm_client.get_api_key", return_value="key")
    def test_timeout_returns_error_string(self, mock_key, mock_urlopen):
        from ai.llm_client import call_llm
        from urllib.error import URLError

        mock_urlopen.side_effect = URLError("timed out")

        result = call_llm("prompt")
        assert "[LLM] Call failed" in result
