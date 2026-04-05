#!/usr/bin/env python3
"""SentinelFO Control Portal."""

import json
import logging
import os
import sys
from functools import wraps

_dir = os.path.dirname(os.path.abspath(__file__))
_root = os.path.dirname(_dir)
if _root not in sys.path:
    sys.path.insert(0, _root)

from flask import Flask, redirect, render_template, request, session, url_for, jsonify
from portal.config import (
    PORTAL_USERNAME, PORTAL_PASSWORD, SECRET_KEY,
    VERSIONS, ARCHITECTURES, BACKENDS, PROVIDERS,
)
from portal import aws_ops, lock

app = Flask(__name__)
app.secret_key = SECRET_KEY
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
        if request.form.get("username") == PORTAL_USERNAME and request.form.get("password") == PORTAL_PASSWORD:
            session["authenticated"] = True
            session["username"] = request.form["username"]
            return redirect(url_for("index"))
        error = "Invalid credentials"
    return render_template("login.html", error=error)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/")
@login_required
def index():
    return render_template("index.html", username=session.get("username", ""),
                           versions=VERSIONS, architectures=ARCHITECTURES,
                           backends=BACKENDS, providers=PROVIDERS)


@app.route("/demo")
@login_required
def demo():
    return render_template("demo.html", username=session.get("username", ""))


# ── API ─────────────────────────────────────────────────────────────────────


@app.route("/api/status")
@login_required
def api_status():
    try:
        return jsonify({"status": aws_ops.get_full_status(), "lock": lock.get_lock_status()})
    except Exception as e:
        logger.error(f"Status: {e}")
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

    if version not in VERSIONS:
        return jsonify({"error": f"Invalid version: {version}"}), 400
    if architecture not in ARCHITECTURES:
        return jsonify({"error": f"Invalid architecture: {architecture}"}), 400
    if backend not in BACKENDS:
        return jsonify({"error": f"Invalid backend: {backend}"}), 400

    config_str = json.dumps({"version": version, "architecture": architecture, "backend": backend, "provider": provider})
    if not lock.acquire_lock(session.get("username", "unknown"), config_str):
        info = lock.get_lock_status()
        return jsonify({"error": f"Test locked by {info.get('locked_by', 'unknown')}"}), 409

    try:
        aws_ops.start_test(version, architecture, backend, provider)
        return jsonify({"ok": True, "message": f"Test started: {version} / {ARCHITECTURES[architecture]['name']} / {BACKENDS[backend]['name']}"})
    except Exception as e:
        lock.release_lock()
        logger.error(f"Start: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/stop", methods=["POST"])
@login_required
def api_stop():
    try:
        aws_ops.stop_test()
        lock.release_lock()
        return jsonify({"ok": True, "message": "Test stopped"})
    except Exception as e:
        logger.error(f"Stop: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/reset", methods=["POST"])
@login_required
def api_reset():
    try:
        result = aws_ops.full_reset()
        lock.release_lock()
        return jsonify(result)
    except Exception as e:
        logger.error(f"Reset: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/trigger", methods=["POST"])
@login_required
def api_trigger():
    try:
        aws_ops.trigger_failover()
        return jsonify({"ok": True, "message": "Failure injected — ECS scaling to 0 in primary"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/aurora/promote", methods=["POST"])
@login_required
def api_aurora_promote():
    try:
        return jsonify(aws_ops.promote_aurora())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/failback", methods=["POST"])
@login_required
def api_failback():
    try:
        return jsonify(aws_ops.invoke_failback(session.get("username", "unknown")))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/auto-promote", methods=["POST"])
@login_required
def api_auto_promote():
    data = request.get_json() or {}
    enabled = data.get("enabled", False)
    try:
        for region in [aws_ops.PRIMARY_REGION, aws_ops.SECONDARY_REGION]:
            aws_ops.set_lambda_env({"AURORA_AUTO_PROMOTE": "true" if enabled else "false"}, region)
        return jsonify({"ok": True, "message": f"Auto-promote {'enabled' if enabled else 'disabled'}"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)
