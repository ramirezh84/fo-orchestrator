#!/usr/bin/env python3
"""Tests for tools/generate_dashboard.py — verifies the scenario-aware
generator emits the right widget set for each deployment shape (issue #100)."""

import json
import os
import sys
from pathlib import Path

import pytest

# Add tools/ to path so we can import generate_dashboard.
_TOOLS = Path(__file__).resolve().parent.parent / "tools"
sys.path.insert(0, str(_TOOLS))

import generate_dashboard as gd  # noqa: E402


# ---------------------------------------------------------------------------
# Config fixtures
# ---------------------------------------------------------------------------

def _base_cfg(**overrides) -> dict:
    """Build a complete config with all required fields populated.

    Tests override scenario flags + any flag-conditional resource fields
    needed for the variant under test.
    """
    cfg = {
        "dashboard_name":        "test-dashboard",
        "account_id":            "123456789012",
        "app_name":              "test-app",
        "orchestrator_function": "test-orchestrator",
        "failback_function":     "test-failback",
        "primary_region":        "us-east-1",
        "secondary_region":      "us-east-2",
        "cw_namespace":          "Custom/TestApp",
        "cw_metric":             "RegionActiveStatus",
        "ecs_cluster":           "test-cluster",
        "ecs_service":           "test-svc",
        "primary_alb_suffix":    "app/test-alb/abc",
        "secondary_alb_suffix":  "app/test-alb/def",
        "primary_tg_suffix":     "targetgroup/test-tg/abc",
        "secondary_tg_suffix":   "targetgroup/test-tg/def",
        "primary_alarm_name":    "test-region-inactive-us-east-1",
        "secondary_alarm_name":  "test-region-inactive-us-east-2",
        "primary_r53_hc_id":     "uuid-1",
        "secondary_r53_hc_id":   "uuid-2",
        # Scenario defaults
        "routing_mode":   "failover",
        "aurora_present": False,
        "aurora_auto":    False,
        "redis_present":  False,
        "redis_auto":     False,
        "api_gw_present": False,
    }
    cfg.update(overrides)
    return cfg


def _aurora_cfg(**overrides):
    cfg = _base_cfg(
        aurora_present=True,
        primary_aurora_cluster="test-aurora-w1",
        secondary_aurora_cluster="test-aurora-w2",
    )
    cfg.update(overrides)
    return cfg


def _aurora_redis_cfg(**overrides):
    cfg = _aurora_cfg(
        redis_present=True,
        primary_redis_rg="test-redis-w1",
        secondary_redis_rg="test-redis-w2",
    )
    cfg.update(overrides)
    return cfg


def _full_cfg(**overrides):
    cfg = _aurora_redis_cfg(
        api_gw_present=True,
        api_gw_name="test-api",
    )
    cfg.update(overrides)
    return cfg


def _widget_titles(dashboard) -> list:
    out = []
    for w in dashboard["widgets"]:
        props = w.get("properties", {})
        if "title" in props:
            out.append(props["title"])
        elif "markdown" in props:
            # Title row uses markdown — extract the first heading line.
            first = props["markdown"].split("\n")[0]
            out.append(first)
    return out


def _all_metric_names(dashboard) -> set:
    """Collect every MetricName referenced across all metric widgets."""
    names = set()
    for w in dashboard["widgets"]:
        if w.get("type") != "metric":
            continue
        for spec in w["properties"].get("metrics", []):
            if isinstance(spec, list) and len(spec) >= 2:
                names.add(spec[1])
    return names


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------

class TestConfigValidation:
    def test_aurora_present_requires_aurora_resource_fields(self, tmp_path):
        cfg = _base_cfg(aurora_present=True)
        # Don't add aurora cluster fields → must fail at load.
        cfg_path = tmp_path / "c.yaml"
        cfg_path.write_text(_yaml_dump(cfg))
        with pytest.raises(SystemExit):
            gd.load_config(str(cfg_path))

    def test_redis_present_requires_redis_resource_fields(self, tmp_path):
        cfg = _aurora_cfg(redis_present=True)
        # Don't add redis RG fields → must fail.
        cfg_path = tmp_path / "c.yaml"
        cfg_path.write_text(_yaml_dump(cfg))
        with pytest.raises(SystemExit):
            gd.load_config(str(cfg_path))

    def test_api_gw_present_requires_api_gw_name(self, tmp_path):
        cfg = _aurora_cfg(api_gw_present=True)
        cfg_path = tmp_path / "c.yaml"
        cfg_path.write_text(_yaml_dump(cfg))
        with pytest.raises(SystemExit):
            gd.load_config(str(cfg_path))

    def test_routing_mode_must_be_valid(self, tmp_path):
        cfg = _aurora_cfg(routing_mode="bogus")
        cfg_path = tmp_path / "c.yaml"
        cfg_path.write_text(_yaml_dump(cfg))
        with pytest.raises(SystemExit):
            gd.load_config(str(cfg_path))

    def test_replace_placeholder_caught(self, tmp_path):
        cfg = _aurora_cfg(primary_aurora_cluster="REPLACE-CLUSTER-NAME")
        cfg_path = tmp_path / "c.yaml"
        cfg_path.write_text(_yaml_dump(cfg))
        with pytest.raises(SystemExit):
            gd.load_config(str(cfg_path))

    def test_scenario_defaults_applied_when_absent(self, tmp_path):
        """Legacy YAML (no scenario flags) loads cleanly with the back-compat
        defaults: routing=failover, aurora_present=True. The resource fields
        for Aurora MUST be present in such a YAML — that was already required
        in the pre-#100 schema, and the default-True for aurora_present is
        designed to keep that legacy YAML working unchanged."""
        cfg = _aurora_cfg()  # has Aurora cluster fields
        for k in ["routing_mode", "aurora_present", "aurora_auto",
                  "redis_present", "redis_auto", "api_gw_present"]:
            cfg.pop(k, None)
        cfg_path = tmp_path / "c.yaml"
        cfg_path.write_text(_yaml_dump(cfg))
        loaded = gd.load_config(str(cfg_path))
        assert loaded["routing_mode"] == "failover"
        assert loaded["aurora_present"] is True  # legacy default
        assert loaded["redis_present"] is False
        assert loaded["api_gw_present"] is False


def _yaml_dump(cfg: dict) -> str:
    """Lightweight YAML emitter — avoids requiring pyyaml when only writing
    test configs. Quotes every string so booleans/ints stay distinct."""
    lines = []
    for k, v in cfg.items():
        if isinstance(v, bool):
            lines.append(f"{k}: {str(v).lower()}")
        elif isinstance(v, (int, float)):
            lines.append(f"{k}: {v}")
        else:
            lines.append(f'{k}: "{v}"')
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Scenario snapshot tests — what shows up for each deployment shape
# ---------------------------------------------------------------------------

class TestAppOnlyStack:
    """No Aurora, no Redis, no API GW. App-only / stateless service."""

    def test_no_aurora_widget(self):
        d = gd.build_dashboard(_base_cfg())
        titles = _widget_titles(d)
        assert not any("Aurora" in t for t in titles), titles

    def test_no_elasticache_widget(self):
        d = gd.build_dashboard(_base_cfg())
        titles = _widget_titles(d)
        assert not any("ElastiCache" in t for t in titles)

    def test_no_api_gw_widget(self):
        d = gd.build_dashboard(_base_cfg())
        titles = _widget_titles(d)
        assert not any("API Gateway" in t for t in titles)

    def test_per_signal_panel_excludes_data_tier_signals(self):
        d = gd.build_dashboard(_base_cfg())
        names = _all_metric_names(d)
        assert "SignalAurora" not in names
        assert "SignalElasticache" not in names
        assert "SignalApiGw" not in names
        # Always-on signals still present.
        assert "SignalHttp" in names
        assert "SignalAlb" in names
        assert "SignalEcs" in names

    def test_promotion_durations_row_omitted_when_no_tier(self):
        d = gd.build_dashboard(_base_cfg())
        titles = _widget_titles(d)
        assert not any("Promotion durations" in t for t in titles)

    def test_lifecycle_counters_row_present_with_failover_only(self):
        d = gd.build_dashboard(_base_cfg())
        titles = _widget_titles(d)
        assert any("Lifecycle counters" in t for t in titles)
        # No promotion counters because no data tier.
        names = _all_metric_names(d)
        assert "FailoversTriggered" in names
        assert "FailbacksCompleted" in names
        assert "PromotionsAttempted" not in names

    def test_scenario_tag_in_title_says_aurora_absent(self):
        d = gd.build_dashboard(_base_cfg())
        title_md = d["widgets"][0]["properties"]["markdown"]
        assert "Aurora-absent" in title_md
        assert "Redis-absent" in title_md
        assert "APIgw-absent" in title_md


class TestAuroraOnlyManual:
    """Aurora present + manual promote, no Redis."""

    def test_aurora_widget_present(self):
        d = gd.build_dashboard(_aurora_cfg())
        titles = _widget_titles(d)
        assert any("Aurora" in t for t in titles)

    def test_no_elasticache_widget(self):
        d = gd.build_dashboard(_aurora_cfg())
        titles = _widget_titles(d)
        assert not any("ElastiCache" in t for t in titles)

    def test_per_signal_includes_aurora_excludes_redis(self):
        d = gd.build_dashboard(_aurora_cfg())
        names = _all_metric_names(d)
        assert "SignalAurora" in names
        assert "SignalElasticache" not in names

    def test_promotion_durations_row_renders_aurora_only(self):
        d = gd.build_dashboard(_aurora_cfg())
        names = _all_metric_names(d)
        assert "AuroraPromotionDurationSeconds" in names
        assert "RedisPromotionDurationSeconds" not in names

    def test_promotion_counters_split_by_tier_aurora_only(self):
        d = gd.build_dashboard(_aurora_cfg())
        # Aurora promotion counters present; Redis counters absent.
        # Find the lifecycle widget that has Tier=Aurora dim.
        promo_dims_seen = []
        for w in d["widgets"]:
            for spec in w.get("properties", {}).get("metrics", []):
                if isinstance(spec, list) and "Tier" in spec:
                    tier_idx = spec.index("Tier")
                    promo_dims_seen.append(spec[tier_idx + 1])
        assert "Aurora" in promo_dims_seen
        assert "Redis" not in promo_dims_seen

    def test_scenario_tag_aurora_manual(self):
        d = gd.build_dashboard(_aurora_cfg())
        title_md = d["widgets"][0]["properties"]["markdown"]
        assert "Aurora-manual" in title_md


class TestAuroraAutoRedisManual:
    """Aurora auto-promote, Redis present + manual."""

    def test_both_data_tier_widgets_present(self):
        d = gd.build_dashboard(_aurora_redis_cfg(aurora_auto=True))
        titles = _widget_titles(d)
        assert any("Aurora" in t for t in titles)
        assert any("ElastiCache" in t for t in titles)

    def test_per_signal_includes_both_data_tiers(self):
        d = gd.build_dashboard(_aurora_redis_cfg(aurora_auto=True))
        names = _all_metric_names(d)
        assert "SignalAurora" in names
        assert "SignalElasticache" in names

    def test_promotion_duration_metrics_for_both_tiers(self):
        d = gd.build_dashboard(_aurora_redis_cfg(aurora_auto=True))
        names = _all_metric_names(d)
        assert "AuroraPromotionDurationSeconds" in names
        assert "RedisPromotionDurationSeconds" in names

    def test_scenario_tag(self):
        d = gd.build_dashboard(_aurora_redis_cfg(aurora_auto=True))
        title_md = d["widgets"][0]["properties"]["markdown"]
        assert "Aurora-auto" in title_md
        assert "Redis-manual" in title_md


class TestActiveActiveVariant:
    """ROUTING_MODE=active-active changes the RegionActiveStatus framing."""

    def test_routing_mode_in_scenario_tag(self):
        d = gd.build_dashboard(_aurora_redis_cfg(routing_mode="active-active",
                                                  aurora_auto=True, redis_auto=True))
        title_md = d["widgets"][0]["properties"]["markdown"]
        assert "active-active" in title_md

    def test_metric_dimensions_carry_active_active_routing_mode(self):
        d = gd.build_dashboard(_aurora_redis_cfg(routing_mode="active-active",
                                                  aurora_auto=True, redis_auto=True))
        # Pick the LatchEngaged metric and confirm RoutingMode dim.
        found = False
        for w in d["widgets"]:
            for spec in w.get("properties", {}).get("metrics", []):
                if (isinstance(spec, list) and len(spec) >= 2
                        and spec[1] == "LatchEngaged"):
                    assert "RoutingMode" in spec
                    rm_idx = spec.index("RoutingMode")
                    assert spec[rm_idx + 1] == "active-active"
                    found = True
        assert found, "LatchEngaged metric not found in dashboard"


class TestFullStack:
    """All scenario flags true — superset for assertion variety."""

    def test_all_data_tier_rows_present(self):
        d = gd.build_dashboard(_full_cfg(aurora_auto=True, redis_auto=True))
        titles = _widget_titles(d)
        assert any("Aurora" in t for t in titles)
        assert any("ElastiCache" in t for t in titles)
        assert any("API Gateway" in t for t in titles)

    def test_all_per_signal_metrics_present(self):
        d = gd.build_dashboard(_full_cfg(aurora_auto=True, redis_auto=True))
        names = _all_metric_names(d)
        for sig in ["SignalHttp", "SignalAlb", "SignalEcs",
                    "SignalApiGw", "SignalAurora", "SignalElasticache"]:
            assert sig in names, f"Missing {sig}"

    def test_app_name_appears_in_metric_dims(self):
        d = gd.build_dashboard(_full_cfg(app_name="my-app",
                                          aurora_auto=True, redis_auto=True))
        any_app_dim = False
        for w in d["widgets"]:
            for spec in w.get("properties", {}).get("metrics", []):
                if isinstance(spec, list) and "AppName" in spec:
                    idx = spec.index("AppName")
                    if spec[idx + 1] == "my-app":
                        any_app_dim = True
        assert any_app_dim, "AppName=my-app dimension never appears in any metric"

    def test_dashboard_widgets_render_as_valid_json(self):
        d = gd.build_dashboard(_full_cfg(aurora_auto=True, redis_auto=True))
        # If json.dumps fails the dashboard couldn't actually be deployed.
        json.dumps(d)


class TestThresholdAnnotation:
    def test_consecutive_failures_threshold_default_3(self):
        d = gd.build_dashboard(_aurora_cfg())
        # Find the consecutive failures widget — its annotation marks 3.
        found = False
        for w in d["widgets"]:
            title = w.get("properties", {}).get("title", "")
            if "Consecutive failures" in title:
                anns = w["properties"].get("annotations", {}).get("horizontal", [])
                assert any(a["value"] == 3 for a in anns)
                found = True
        assert found, "Consecutive failures widget not generated"

    def test_consecutive_failures_threshold_overrideable(self):
        d = gd.build_dashboard(_aurora_cfg(consecutive_failures_threshold=5))
        for w in d["widgets"]:
            title = w.get("properties", {}).get("title", "")
            if "Consecutive failures" in title:
                anns = w["properties"].get("annotations", {}).get("horizontal", [])
                assert any(a["value"] == 5 for a in anns)
                return
        pytest.fail("Consecutive failures widget not generated")
