#!/usr/bin/env python3
"""
SentinelFO Control Portal
===========================
Local Flask app for managing failover orchestrator test environments.

Usage:
  pip install flask boto3
  python3 portal/app.py

  Then open: http://localhost:5001
"""

import json
import logging
from functools import wraps

from flask import Flask, Response, redirect, render_template, request, session, url_for, jsonify

import os
import sys

# Allow running from project root or from portal/ directory
_dir = os.path.dirname(os.path.abspath(__file__))
_root = os.path.dirname(_dir)
if _root not in sys.path:
    sys.path.insert(0, _root)

from portal.config import (
    PORTAL_USERNAME, PORTAL_PASSWORD, SECRET_KEY,
    VERSIONS, ARCHITECTURES, BACKENDS, PROVIDERS,
)
from portal import aws_ops, lock

app = Flask(__name__)
app.secret_key = SECRET_KEY

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ── Auth ────────────────────────────────────────────────────────────────────────


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("authenticated"):
            if request.path.startswith("/api/"):
                return jsonify({"error": "Not authenticated"}), 401
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated


@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        username = request.form.get("username", "")
        password = request.form.get("password", "")
        if username == PORTAL_USERNAME and password == PORTAL_PASSWORD:
            session["authenticated"] = True
            session["username"] = username
            return redirect(url_for("index"))
        error = "Invalid credentials"
    return render_template("login.html", error=error)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


# ── Pages ───────────────────────────────────────────────────────────────────────


@app.route("/")
@login_required
def index():
    return render_template(
        "index.html",
        username=session.get("username", ""),
        versions=VERSIONS,
        architectures=ARCHITECTURES,
        backends=BACKENDS,
        providers=PROVIDERS,
    )


@app.route("/demo")
@login_required
def demo():
    return render_template(
        "demo.html",
        username=session.get("username", ""),
    )


# ── API ─────────────────────────────────────────────────────────────────────────


@app.route("/api/status")
@login_required
def api_status():
    try:
        status = aws_ops.get_full_status()
        lock_status = lock.get_lock_status()
        return jsonify({"status": status, "lock": lock_status})
    except Exception as e:
        logger.error(f"Status error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/versions")
@login_required
def api_versions():
    return jsonify(VERSIONS)


@app.route("/api/start", methods=["POST"])
@login_required
def api_start():
    data = request.get_json() or {}
    version = data.get("version", "v1.2")
    architecture = data.get("architecture", "active-passive")
    backend = data.get("backend", "dynamodb")
    provider = data.get("provider", "claude")

    # Validate
    if version not in VERSIONS:
        return jsonify({"error": f"Invalid version: {version}"}), 400
    if architecture not in ARCHITECTURES:
        return jsonify({"error": f"Invalid architecture: {architecture}"}), 400
    if backend not in BACKENDS:
        return jsonify({"error": f"Invalid backend: {backend}"}), 400

    # Check Aurora — both regions must have available instances
    aurora = aws_ops.get_aurora_status()
    w1_ok = aurora.get(aws_ops.PRIMARY_REGION) == "available"
    w2_ok = aurora.get(aws_ops.SECONDARY_REGION) == "available"
    if not (w1_ok and w2_ok):
        missing = []
        if not w1_ok:
            missing.append(f"us-west-1: {aurora.get(aws_ops.PRIMARY_REGION, 'unknown')}")
        if not w2_ok:
            missing.append(f"us-west-2: {aurora.get(aws_ops.SECONDARY_REGION, 'unknown')}")
        return jsonify({"error": f"Aurora not ready in both regions. Turn on Aurora first. Status: {', '.join(missing)}"}), 400

    # Acquire lock
    config_str = json.dumps({"version": version, "architecture": architecture, "backend": backend, "provider": provider})
    if not lock.acquire_lock(session.get("username", "unknown"), config_str):
        lock_info = lock.get_lock_status()
        return jsonify({"error": f"Test locked by {lock_info.get('locked_by', 'unknown')}"}), 409

    try:
        aws_ops.start_test(version, architecture, backend, provider)
        return jsonify({"ok": True, "message": f"Test started: {version} / {ARCHITECTURES[architecture]['name']} / {BACKENDS[backend]['name']}"})
    except Exception as e:
        lock.release_lock()
        logger.error(f"Start error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/stop", methods=["POST"])
@login_required
def api_stop():
    try:
        aws_ops.stop_test()
        lock.release_lock()
        return jsonify({"ok": True, "message": "Test stopped"})
    except Exception as e:
        logger.error(f"Stop error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/aurora/on", methods=["POST"])
@login_required
def api_aurora_on():
    try:
        errors = aws_ops.create_aurora_instances()
        if errors:
            return jsonify({"ok": False, "errors": errors}), 500
        return jsonify({"ok": True, "message": "Aurora instances creating (~5 min)"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/aurora/off", methods=["POST"])
@login_required
def api_aurora_off():
    try:
        errors = aws_ops.delete_aurora_instances()
        if errors:
            return jsonify({"ok": False, "errors": errors}), 500
        return jsonify({"ok": True, "message": "Aurora instances deleting"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/trigger", methods=["POST"])
@login_required
def api_trigger():
    try:
        aws_ops.trigger_failover()
        return jsonify({"ok": True, "message": "Failure injected. Failover in ~3 minutes."})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/auto-promote", methods=["POST"])
@login_required
def api_auto_promote():
    """Toggle AURORA_AUTO_PROMOTE env var on the Lambda."""
    data = request.get_json() or {}
    enabled = data.get("enabled", False)
    try:
        for region in [aws_ops.PRIMARY_REGION, aws_ops.SECONDARY_REGION]:
            aws_ops.update_lambda_env(
                {"AURORA_AUTO_PROMOTE": "true" if enabled else "false"},
                region
            )
        return jsonify({"ok": True, "message": f"Aurora auto-promote {'enabled' if enabled else 'disabled'}"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/aurora/promote", methods=["POST"])
@login_required
def api_aurora_promote():
    """Trigger Aurora switchover to secondary region."""
    try:
        result = aws_ops.promote_aurora()
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/failback", methods=["POST"])
@login_required
def api_failback():
    """Invoke the failback Lambda to return traffic to primary."""
    try:
        result = aws_ops.invoke_failback(session.get("username", "unknown"))
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Main ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)
