"""Tests for EventBridge: event buses, rules, targets, and event routing."""

import json

import pytest

from openaws.errors import Conflict, NotFound, ValidationError


# ---------------------------------------------------------------------------
# Event buses
# ---------------------------------------------------------------------------

def test_create_list_delete_bus(app):
    b = app.eventbridge.create_event_bus("my-bus")
    assert b["name"] == "my-bus"
    assert "arn" in b
    buses = app.eventbridge.list_event_buses()
    assert any(x["name"] == "my-bus" for x in buses)
    app.eventbridge.delete_event_bus("my-bus")
    assert all(x["name"] != "my-bus" for x in app.eventbridge.list_event_buses())


def test_create_duplicate_bus_raises(app):
    app.eventbridge.create_event_bus("b")
    with pytest.raises(Conflict):
        app.eventbridge.create_event_bus("b")


def test_delete_default_bus_raises(app):
    with pytest.raises(ValidationError):
        app.eventbridge.delete_event_bus("default")


# ---------------------------------------------------------------------------
# Rules
# ---------------------------------------------------------------------------

def test_put_and_list_rule(app):
    r = app.eventbridge.put_rule(
        "my-rule",
        event_pattern={"source": ["myapp"]},
    )
    assert r["name"] == "my-rule"
    rules = app.eventbridge.list_rules()
    assert any(x["name"] == "my-rule" for x in rules)


def test_put_rule_requires_pattern_or_schedule(app):
    with pytest.raises(ValidationError):
        app.eventbridge.put_rule("bad-rule")


def test_put_rule_upserts(app):
    app.eventbridge.put_rule("r", event_pattern={"source": ["a"]})
    app.eventbridge.put_rule("r", event_pattern={"source": ["b"]}, state="DISABLED")
    rules = app.eventbridge.list_rules()
    match = [x for x in rules if x["name"] == "r"][0]
    assert match["state"] == "DISABLED"
    assert match["event_pattern"]["source"] == ["b"]


def test_delete_rule(app):
    app.eventbridge.put_rule("r", event_pattern={"source": ["x"]})
    app.eventbridge.delete_rule("r")
    assert app.eventbridge.list_rules() == []


def test_delete_unknown_rule_raises(app):
    with pytest.raises(NotFound):
        app.eventbridge.delete_rule("ghost")


# ---------------------------------------------------------------------------
# Targets
# ---------------------------------------------------------------------------

def test_put_and_list_targets(app):
    app.eventbridge.put_rule("r", event_pattern={"source": ["x"]})
    app.eventbridge.put_targets("r", targets=[
        {"id": "t1", "type": "sqs", "arn": "arn:openaws:sqs:local:000:myqueue"},
    ])
    targets = app.eventbridge.list_targets("r")
    assert len(targets) == 1
    assert targets[0]["id"] == "t1"


def test_remove_targets(app):
    app.eventbridge.put_rule("r", event_pattern={"source": ["x"]})
    app.eventbridge.put_targets("r", targets=[
        {"id": "t1", "type": "sqs", "arn": "arn:openaws:sqs:local:000:q"},
        {"id": "t2", "type": "lambda", "arn": "arn:openaws:lambda:local:000:fn"},
    ])
    app.eventbridge.remove_targets("r", ids=["t1"])
    remaining = app.eventbridge.list_targets("r")
    assert len(remaining) == 1
    assert remaining[0]["id"] == "t2"


# ---------------------------------------------------------------------------
# Pattern matching
# ---------------------------------------------------------------------------

def test_event_pattern_source_match(app):
    received = []
    app.lambdas.register_callable("sink", lambda e, c: received.append(e["source"]))
    app.eventbridge.put_rule("r", event_pattern={"source": ["myapp"]})
    app.eventbridge.put_targets("r", targets=[
        {"id": "t1", "type": "lambda", "arn": "arn:openaws:lambda:local:000:function/sink"},
    ])
    app.eventbridge.put_events([
        {"source": "myapp", "detail_type": "X", "detail": {}}
    ])
    assert received == ["myapp"]


def test_event_pattern_no_match_skipped(app):
    received = []
    app.lambdas.register_callable("sink", lambda e, c: received.append(1))
    app.eventbridge.put_rule("r", event_pattern={"source": ["other-app"]})
    app.eventbridge.put_targets("r", targets=[
        {"id": "t1", "type": "lambda", "arn": "arn:openaws:lambda:local:000:function/sink"},
    ])
    app.eventbridge.put_events([{"source": "myapp", "detail_type": "X", "detail": {}}])
    assert received == []


def test_disabled_rule_not_fired(app):
    received = []
    app.lambdas.register_callable("sink", lambda e, c: received.append(1))
    app.eventbridge.put_rule(
        "r", event_pattern={"source": ["myapp"]}, state="DISABLED"
    )
    app.eventbridge.put_targets("r", targets=[
        {"id": "t1", "type": "lambda", "arn": "arn:openaws:lambda:local:000:function/sink"},
    ])
    app.eventbridge.put_events([{"source": "myapp", "detail_type": "X", "detail": {}}])
    assert received == []


def test_event_routes_to_sqs_target(app):
    app.sqs.create_queue("eb-inbox")
    app.eventbridge.put_rule("r", event_pattern={"source": ["svc"]})
    app.eventbridge.put_targets("r", targets=[
        {"id": "t1", "type": "sqs", "arn": "arn:openaws:sqs:local:000:eb-inbox"},
    ])
    app.eventbridge.put_events([{"source": "svc", "detail_type": "T", "detail": {}}])
    assert app.sqs.message_count("eb-inbox") == 1


def test_detail_field_pattern_match(app):
    received = []
    app.lambdas.register_callable("sink", lambda e, c: received.append(e["detail"]["status"]))
    app.eventbridge.put_rule(
        "r", event_pattern={"source": ["svc"], "detail": {"status": ["active"]}}
    )
    app.eventbridge.put_targets("r", targets=[
        {"id": "t1", "type": "lambda", "arn": "arn:openaws:lambda:local:000:function/sink"},
    ])
    # matching
    app.eventbridge.put_events([{"source": "svc", "detail_type": "T", "detail": {"status": "active"}}])
    # non-matching
    app.eventbridge.put_events([{"source": "svc", "detail_type": "T", "detail": {"status": "inactive"}}])
    assert received == ["active"]


def test_put_events_returns_entries(app):
    result = app.eventbridge.put_events([
        {"source": "a", "detail_type": "B", "detail": {}}
    ])
    assert result["failed_entry_count"] == 0
    assert len(result["entries"]) == 1
    assert "event_id" in result["entries"][0]


# ---------------------------------------------------------------------------
# HTTP server round-trip
# ---------------------------------------------------------------------------

def test_eventbridge_via_http(server):
    import urllib.request

    base = server.base_url

    def post(payload):
        data = json.dumps(payload).encode()
        req = urllib.request.Request(
            f"{base}/eventbridge",
            data=data,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read())

    post({"action": "put_rule", "name": "r1", "event_pattern": {"source": ["svc"]}})
    rules = post({"action": "list_rules"})
    assert any(r["name"] == "r1" for r in rules["rules"])

    post({"action": "put_targets", "rule": "r1",
          "targets": [{"id": "t1", "type": "sqs", "arn": "arn:openaws:sqs:local:000:q"}]})
    result = post({"action": "put_events",
                   "events": [{"source": "svc", "detail_type": "T", "detail": {}}]})
    assert result["failed_entry_count"] == 0

    post({"action": "delete_rule", "name": "r1"})
    assert post({"action": "list_rules"})["rules"] == []
