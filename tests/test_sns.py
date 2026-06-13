"""Tests for SNS: topics, subscriptions, publish / fan-out."""

import json

import pytest

from openaws.errors import Conflict, NotFound, ValidationError


# ---------------------------------------------------------------------------
# Topics
# ---------------------------------------------------------------------------

def test_create_list_delete_topic(app):
    t = app.sns.create_topic("events")
    assert t["name"] == "events"
    assert "arn" in t
    topics = app.sns.list_topics()
    assert any(t["name"] == "events" for t in topics)
    app.sns.delete_topic("events")
    assert all(t["name"] != "events" for t in app.sns.list_topics())


def test_duplicate_topic_raises(app):
    app.sns.create_topic("dup")
    with pytest.raises(Conflict):
        app.sns.create_topic("dup")


def test_delete_unknown_topic_raises(app):
    with pytest.raises(NotFound):
        app.sns.delete_topic("ghost")


# ---------------------------------------------------------------------------
# Subscriptions
# ---------------------------------------------------------------------------

def test_subscribe_and_list(app):
    app.sns.create_topic("news")
    sub = app.sns.subscribe("news", "log", "logger-1")
    assert sub["subscription_arn"]
    subs = app.sns.list_subscriptions("news")
    assert len(subs) == 1
    assert subs[0]["protocol"] == "log"
    assert subs[0]["endpoint"] == "logger-1"


def test_subscribe_unknown_topic_raises(app):
    with pytest.raises(NotFound):
        app.sns.subscribe("ghost", "log", "ep")


def test_subscribe_bad_protocol_raises(app):
    app.sns.create_topic("t")
    with pytest.raises(ValidationError):
        app.sns.subscribe("t", "email", "user@example.com")


def test_list_subscriptions_all(app):
    app.sns.create_topic("t1")
    app.sns.create_topic("t2")
    app.sns.subscribe("t1", "log", "ep1")
    app.sns.subscribe("t2", "log", "ep2")
    all_subs = app.sns.list_subscriptions()
    assert len(all_subs) == 2


def test_unsubscribe(app):
    app.sns.create_topic("t")
    sub = app.sns.subscribe("t", "log", "ep")
    app.sns.unsubscribe(sub["subscription_arn"])
    assert len(app.sns.list_subscriptions("t")) == 0


def test_unsubscribe_unknown_raises(app):
    with pytest.raises(NotFound):
        app.sns.unsubscribe("arn:openaws:sns:local:000000000000:topic:fake")


# ---------------------------------------------------------------------------
# Publish (log protocol)
# ---------------------------------------------------------------------------

def test_publish_log_protocol(app):
    app.sns.create_topic("alerts")
    app.sns.subscribe("alerts", "log", "logger")
    result = app.sns.publish("alerts", "hello world", subject="test")
    assert result["delivered"] == 1
    deliveries = app.sns.get_deliveries("alerts")
    assert len(deliveries) == 1
    payload = json.loads(deliveries[0]["payload"])
    assert payload["Message"] == "hello world"
    assert payload["Subject"] == "test"


def test_publish_fan_out_multiple_subscribers(app):
    app.sns.create_topic("news")
    app.sns.subscribe("news", "log", "l1")
    app.sns.subscribe("news", "log", "l2")
    r = app.sns.publish("news", "breaking news")
    assert r["delivered"] == 2


def test_publish_to_sqs_subscriber(app):
    app.sqs.create_queue("inbox")
    app.sns.create_topic("events")
    app.sns.subscribe("events", "sqs", "inbox")
    app.sns.publish("events", "event-body")
    assert app.sqs.message_count("inbox") == 1
    msg = app.sqs.receive_messages("inbox")[0]
    payload = json.loads(msg["body"])
    assert payload["Message"] == "event-body"


def test_publish_to_lambda_subscriber(app):
    received = []
    app.lambdas.register_callable("on_event", lambda e, c: received.extend(
        r["Sns"]["Message"] for r in e["Records"]
    ))
    app.sns.create_topic("evt")
    app.sns.subscribe("evt", "lambda", "on_event")
    app.sns.publish("evt", "the-message")
    assert "the-message" in received


def test_publish_with_attributes(app):
    app.sns.create_topic("t")
    app.sns.subscribe("t", "log", "ep")
    app.sns.publish("t", "msg", attributes={"key": {"DataType": "String", "StringValue": "v"}})
    d = app.sns.get_deliveries("t")
    payload = json.loads(d[0]["payload"])
    assert "key" in payload["MessageAttributes"]


def test_delete_topic_removes_subscriptions(app):
    app.sns.create_topic("t")
    app.sns.subscribe("t", "log", "ep")
    app.sns.delete_topic("t")
    # No error; subscriptions are gone
    assert app.sns.list_subscriptions() == []


# ---------------------------------------------------------------------------
# HTTP server round-trip
# ---------------------------------------------------------------------------

def test_sns_via_http(server):
    import urllib.request

    base = server.base_url

    def post(payload):
        data = json.dumps(payload).encode()
        req = urllib.request.Request(
            f"{base}/sns",
            data=data,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read())

    post({"action": "create_topic", "name": "alerts"})
    post({"action": "subscribe", "topic": "alerts", "protocol": "log", "endpoint": "sink"})
    r = post({"action": "publish", "topic": "alerts", "message": "fire!"})
    assert r["delivered"] == 1
    deliveries = post({"action": "get_deliveries", "topic": "alerts"})
    assert len(deliveries["deliveries"]) == 1

    topics = post({"action": "list_topics"})
    assert any(t["name"] == "alerts" for t in topics["topics"])

    subs = post({"action": "list_subscriptions"})
    assert len(subs["subscriptions"]) == 1
    arn = subs["subscriptions"][0]["arn"]
    post({"action": "unsubscribe", "subscription_arn": arn})
    assert post({"action": "list_subscriptions"})["subscriptions"] == []

    post({"action": "delete_topic", "name": "alerts"})
    assert post({"action": "list_topics"})["topics"] == []
