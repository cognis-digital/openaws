import time

import pytest

from openaws.errors import Conflict, NotFound, ValidationError


def test_create_list_delete_queue(app):
    app.sqs.create_queue("jobs")
    assert app.sqs.list_queues() == ["jobs"]
    app.sqs.delete_queue("jobs")
    assert app.sqs.list_queues() == []


def test_duplicate_queue_conflicts(app):
    app.sqs.create_queue("q")
    with pytest.raises(Conflict):
        app.sqs.create_queue("q")


def test_send_receive_delete(app):
    app.sqs.create_queue("jobs")
    sent = app.sqs.send_message("jobs", "hello")
    assert sent["message_id"]
    msgs = app.sqs.receive_messages("jobs")
    assert len(msgs) == 1
    assert msgs[0]["body"] == "hello"
    assert msgs[0]["received_count"] == 1
    deleted = app.sqs.delete_message("jobs", msgs[0]["receipt_handle"])
    assert deleted is True
    assert app.sqs.message_count("jobs") == 0


def test_visibility_timeout_hides_then_redelivers(app):
    app.sqs.create_queue("jobs", visibility_timeout=0.3)
    app.sqs.send_message("jobs", "x")
    first = app.sqs.receive_messages("jobs")
    assert len(first) == 1
    # immediately invisible
    assert app.sqs.receive_messages("jobs") == []
    time.sleep(0.4)
    again = app.sqs.receive_messages("jobs")
    assert len(again) == 1
    assert again[0]["received_count"] == 2


def test_receive_max_messages(app):
    app.sqs.create_queue("jobs")
    for i in range(5):
        app.sqs.send_message("jobs", f"m{i}")
    msgs = app.sqs.receive_messages("jobs", max_messages=3)
    assert len(msgs) == 3
    bodies = [m["body"] for m in msgs]
    assert bodies == ["m0", "m1", "m2"]  # FIFO-ish by created_at


def test_delete_with_bad_handle_returns_false(app):
    app.sqs.create_queue("jobs")
    app.sqs.send_message("jobs", "x")
    assert app.sqs.delete_message("jobs", "not-a-handle") is False


def test_message_count(app):
    app.sqs.create_queue("jobs")
    app.sqs.send_message("jobs", "a")
    app.sqs.send_message("jobs", "b")
    assert app.sqs.message_count("jobs") == 2


def test_missing_queue_raises(app):
    with pytest.raises(NotFound):
        app.sqs.send_message("nope", "x")


def test_body_must_be_string(app):
    app.sqs.create_queue("jobs")
    with pytest.raises(ValidationError):
        app.sqs.send_message("jobs", {"not": "a string"})
