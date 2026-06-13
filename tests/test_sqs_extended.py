"""Tests for SQS extended features: FIFO, DLQ redrive, message attributes."""

import time

import pytest

from openaws.errors import Conflict, NotFound, ValidationError


# ---------------------------------------------------------------------------
# FIFO queues
# ---------------------------------------------------------------------------

def test_fifo_queue_create(app):
    q = app.sqs.create_queue("orders.fifo", fifo=True)
    assert q["fifo"] is True
    assert "orders.fifo" in app.sqs.list_queues()


def test_fifo_requires_dotfifo_suffix(app):
    with pytest.raises(ValidationError):
        app.sqs.create_queue("orders", fifo=True)


def test_fifo_send_requires_group_id(app):
    app.sqs.create_queue("tasks.fifo", fifo=True)
    with pytest.raises(ValidationError):
        app.sqs.send_message("tasks.fifo", "body")


def test_fifo_send_receive(app):
    app.sqs.create_queue("q.fifo", fifo=True)
    app.sqs.send_message("q.fifo", "msg1", message_group_id="grp1")
    app.sqs.send_message("q.fifo", "msg2", message_group_id="grp1")
    msgs = app.sqs.receive_messages("q.fifo", max_messages=2)
    assert len(msgs) == 2
    assert msgs[0]["body"] == "msg1"
    assert msgs[1]["body"] == "msg2"


def test_fifo_deduplication_within_window(app):
    app.sqs.create_queue("dd.fifo", fifo=True, dedup_window=300.0)
    r1 = app.sqs.send_message(
        "dd.fifo", "body", message_group_id="g", message_deduplication_id="uid-1"
    )
    r2 = app.sqs.send_message(
        "dd.fifo", "body", message_group_id="g", message_deduplication_id="uid-1"
    )
    assert r1["message_id"] == r2["message_id"]
    assert r2.get("duplicate") is True
    assert app.sqs.message_count("dd.fifo") == 1


def test_fifo_deduplication_different_ids(app):
    app.sqs.create_queue("dd2.fifo", fifo=True)
    app.sqs.send_message(
        "dd2.fifo", "a", message_group_id="g", message_deduplication_id="id-a"
    )
    app.sqs.send_message(
        "dd2.fifo", "b", message_group_id="g", message_deduplication_id="id-b"
    )
    assert app.sqs.message_count("dd2.fifo") == 2


# ---------------------------------------------------------------------------
# Dead-letter queues
# ---------------------------------------------------------------------------

def test_dlq_create_requires_existing_target(app):
    with pytest.raises(NotFound):
        app.sqs.create_queue("main", dlq_name="missing-dlq", max_receive_count=3)


def test_dlq_redrive_on_exceed(app):
    """Message redriven to DLQ when received_count exceeds max_receive_count."""
    app.sqs.create_queue("dlq")
    # Use a short visibility timeout so messages quickly become visible again
    app.sqs.create_queue("main", visibility_timeout=0.1, dlq_name="dlq", max_receive_count=2)
    app.sqs.send_message("main", "bad-msg")

    # First receive (received_count → 1, within limit)
    m1 = app.sqs.receive_messages("main")
    assert len(m1) == 1
    time.sleep(0.15)  # let visibility timeout expire

    # Second receive (received_count → 2, at limit — still served)
    m2 = app.sqs.receive_messages("main")
    assert len(m2) == 1
    time.sleep(0.15)

    # Third receive (received_count would be 3 > max=2) → redriven to DLQ, not returned
    m3 = app.sqs.receive_messages("main")
    assert len(m3) == 0
    assert app.sqs.message_count("dlq") == 1
    assert app.sqs.message_count("main") == 0


# ---------------------------------------------------------------------------
# Message attributes
# ---------------------------------------------------------------------------

def test_send_with_attributes(app):
    app.sqs.create_queue("q")
    attrs = {
        "color": {"data_type": "String", "string_value": "blue"},
        "count": {"data_type": "Number", "string_value": "42"},
    }
    app.sqs.send_message("q", "hello", attributes=attrs)
    msgs = app.sqs.receive_messages("q")
    assert msgs[0]["attributes"]["color"]["string_value"] == "blue"
    assert msgs[0]["attributes"]["count"]["string_value"] == "42"


def test_get_queue_attributes(app):
    app.sqs.create_queue("q", visibility_timeout=60.0)
    app.sqs.send_message("q", "x")
    attrs = app.sqs.get_queue_attributes("q")
    assert attrs["visibility_timeout"] == 60.0
    assert attrs["message_count"] == 1


# ---------------------------------------------------------------------------
# HTTP server round-trip
# ---------------------------------------------------------------------------

def test_sqs_fifo_via_http(server):
    import json
    import urllib.request

    base = server.base_url

    def post(action_payload):
        data = json.dumps(action_payload).encode()
        req = urllib.request.Request(
            f"{base}/sqs",
            data=data,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read())

    post({"action": "create_queue", "name": "http.fifo", "fifo": True})
    post({"action": "send_message", "queue": "http.fifo", "body": "hi",
          "message_group_id": "g1"})
    resp = post({"action": "receive_messages", "queue": "http.fifo"})
    assert resp["messages"][0]["body"] == "hi"
