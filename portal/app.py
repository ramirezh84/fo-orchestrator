#!/usr/bin/env python3
"""SentinelFO Control Portal — stack-aware."""

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
    VERSIONS, ARCHITECTURES, BACKENDS, PROVIDERS, STACKS, BOTH_REGIONS,
)
from portal import aws_ops, lock

app = Flask(__name__)
app.secret_key = SECRET_KEY
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

VALID_STACKS = list(STACKS.keys())


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
            return redirect(url_for("landing"))
        error = "Invalid credentials"
    return render_template("login.html", error=error)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


# ── Landing Page ────────────────────────────────────────────────────────────


@app.route("/")
@login_required
def landing():
    return render_template("landing.html", username=session.get("username", ""), stacks=STACKS)


# ── Stack Admin + Demo Pages ────────────────────────────────────────────────


@app.route("/<stack_id>")
@login_required
def admin(stack_id):
    if stack_id not in VALID_STACKS:
        return redirect(url_for("landing"))
    return render_template("index.html",
        username=session.get("username", ""),
        stack_id=stack_id,
        stack=STACKS[stack_id],
        stacks=STACKS,
        versions=VERSIONS,
        architectures=ARCHITECTURES,
        providers=PROVIDERS)


@app.route("/<stack_id>/demo")
@login_required
def demo(stack_id):
    if stack_id not in VALID_STACKS:
        return redirect(url_for("landing"))
    return render_template("demo.html",
        username=session.get("username", ""),
        stack_id=stack_id,
        stack=STACKS[stack_id],
        stacks=STACKS)


# ── Stack-Aware API ─────────────────────────────────────────────────────────


@app.route("/api/<stack_id>/status")
@login_required
def api_status(stack_id):
    if stack_id not in VALID_STACKS:
        return jsonify({"error": "Invalid stack"}), 400
    try:
        return jsonify({"status": aws_ops.get_full_status(stack_id), "lock": lock.get_lock_status()})
    except Exception as e:
        logger.error(f"Status: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/<stack_id>/start", methods=["POST"])
@login_required
def api_start(stack_id):
    if stack_id not in VALID_STACKS:
        return jsonify({"error": "Invalid stack"}), 400
    data = request.get_json() or {}
    version = data.get("version", "v1.2")
    architecture = data.get("architecture", "active-passive")
    provider = data.get("provider", "claude")

    if version not in VERSIONS:
        return jsonify({"error": f"Invalid version: {version}"}), 400
    if architecture not in ARCHITECTURES:
        return jsonify({"error": f"Invalid architecture: {architecture}"}), 400

    config_str = json.dumps({"stack": stack_id, "version": version, "architecture": architecture, "provider": provider})
    if not lock.acquire_lock(session.get("username", "unknown"), config_str):
        info = lock.get_lock_status()
        return jsonify({"error": f"Test locked by {info.get('locked_by', 'unknown')}"}), 409

    try:
        aws_ops.start_test(stack_id, version, architecture, None, provider)
        s = STACKS[stack_id]
        return jsonify({"ok": True, "message": f"Test started on {s['name']}: {version} / {ARCHITECTURES[architecture]['name']}"})
    except Exception as e:
        lock.release_lock()
        logger.error(f"Start: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/<stack_id>/stop", methods=["POST"])
@login_required
def api_stop(stack_id):
    if stack_id not in VALID_STACKS:
        return jsonify({"error": "Invalid stack"}), 400
    try:
        aws_ops.stop_test(stack_id)
        lock.release_lock()
        return jsonify({"ok": True, "message": "Test stopped"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/<stack_id>/reset", methods=["POST"])
@login_required
def api_reset(stack_id):
    if stack_id not in VALID_STACKS:
        return jsonify({"error": "Invalid stack"}), 400
    try:
        result = aws_ops.full_reset(stack_id)
        lock.release_lock()
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/<stack_id>/trigger", methods=["POST"])
@login_required
def api_trigger(stack_id):
    if stack_id not in VALID_STACKS:
        return jsonify({"error": "Invalid stack"}), 400
    try:
        aws_ops.trigger_failover(stack_id)
        return jsonify({"ok": True, "message": "Failure injected"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/<stack_id>/aurora/promote", methods=["POST"])
@login_required
def api_aurora_promote(stack_id):
    if stack_id not in VALID_STACKS:
        return jsonify({"error": "Invalid stack"}), 400
    try:
        return jsonify(aws_ops.promote_aurora(stack_id))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/<stack_id>/failback", methods=["POST"])
@login_required
def api_failback(stack_id):
    if stack_id not in VALID_STACKS:
        return jsonify({"error": "Invalid stack"}), 400
    try:
        return jsonify(aws_ops.invoke_failback(stack_id, session.get("username", "unknown")))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/<stack_id>/auto-promote", methods=["POST"])
@login_required
def api_auto_promote(stack_id):
    if stack_id not in VALID_STACKS:
        return jsonify({"error": "Invalid stack"}), 400
    data = request.get_json() or {}
    enabled = data.get("enabled", False)
    try:
        for region in BOTH_REGIONS:
            aws_ops.set_lambda_env(stack_id, {"AURORA_AUTO_PROMOTE": "true" if enabled else "false"}, region)
        return jsonify({"ok": True, "message": f"Auto-promote {'enabled' if enabled else 'disabled'}"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/versions")
@login_required
def api_versions():
    return jsonify(VERSIONS)


if __name__ == "__main__":
    from portal.config import BOTH_REGIONS
    app.run(host="0.0.0.0", port=5001, debug=True)
