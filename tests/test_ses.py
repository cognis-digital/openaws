"""Tests for SES: email identity management and send/capture."""

import json

import pytest

from openaws.errors import Conflict, NotFound, ValidationError


# ---------------------------------------------------------------------------
# Identities
# ---------------------------------------------------------------------------

def test_verify_list_delete_identity(app):
    app.ses.verify_email_identity("alice@example.com")
    assert "alice@example.com" in app.ses.list_identities()
    app.ses.delete_identity("alice@example.com")
    assert "alice@example.com" not in app.ses.list_identities()


def test_duplicate_identity_raises(app):
    app.ses.verify_email_identity("bob@example.com")
    with pytest.raises(Conflict):
        app.ses.verify_email_identity("bob@example.com")


def test_invalid_email_raises(app):
    with pytest.raises(ValidationError):
        app.ses.verify_email_identity("not-an-email")


def test_delete_unknown_identity_raises(app):
    with pytest.raises(NotFound):
        app.ses.delete_identity("ghost@example.com")


# ---------------------------------------------------------------------------
# send_email
# ---------------------------------------------------------------------------

def test_send_email_captured(app):
    result = app.ses.send_email(
        source="sender@example.com",
        to_addresses=["recipient@example.com"],
        subject="Hello",
        body_text="Hello world",
    )
    assert result["message_id"].endswith("@openaws.local")
    emails = app.ses.list_emails()
    assert len(emails) == 1
    assert emails[0]["subject"] == "Hello"
    assert emails[0]["source"] == "sender@example.com"
    assert "recipient@example.com" in emails[0]["to"]


def test_send_email_html(app):
    app.ses.send_email(
        source="s@example.com",
        to_addresses=["r@example.com"],
        subject="HTML",
        body_html="<h1>hi</h1>",
    )
    emails = app.ses.list_emails()
    assert emails[0]["body_html"] == "<h1>hi</h1>"


def test_send_email_with_cc_bcc_reply(app):
    app.ses.send_email(
        source="s@example.com",
        to_addresses=["r@example.com"],
        subject="Full",
        body_text="text",
        cc_addresses=["cc@example.com"],
        bcc_addresses=["bcc@example.com"],
        reply_to=["reply@example.com"],
    )
    email = app.ses.list_emails()[0]
    assert email["cc"] == ["cc@example.com"]
    assert email["bcc"] == ["bcc@example.com"]
    assert email["reply_to"] == ["reply@example.com"]


def test_send_email_missing_source_raises(app):
    with pytest.raises(ValidationError):
        app.ses.send_email("", ["r@example.com"], "S", body_text="x")


def test_send_email_missing_to_raises(app):
    with pytest.raises(ValidationError):
        app.ses.send_email("s@example.com", [], "S", body_text="x")


def test_send_email_missing_subject_raises(app):
    with pytest.raises(ValidationError):
        app.ses.send_email("s@example.com", ["r@example.com"], "", body_text="x")


def test_send_email_missing_body_raises(app):
    with pytest.raises(ValidationError):
        app.ses.send_email("s@example.com", ["r@example.com"], "S")


# ---------------------------------------------------------------------------
# list and get emails
# ---------------------------------------------------------------------------

def test_list_emails_filter_by_to(app):
    app.ses.send_email("s@e.com", ["alice@e.com"], "for alice", body_text="x")
    app.ses.send_email("s@e.com", ["bob@e.com"], "for bob", body_text="y")
    alice_emails = app.ses.list_emails(to_address="alice@e.com")
    assert len(alice_emails) == 1
    assert alice_emails[0]["subject"] == "for alice"


def test_list_emails_limit(app):
    for i in range(5):
        app.ses.send_email("s@e.com", ["r@e.com"], f"msg {i}", body_text="x")
    emails = app.ses.list_emails(limit=3)
    assert len(emails) == 3


def test_get_email_by_id(app):
    result = app.ses.send_email("s@e.com", ["r@e.com"], "find me", body_text="x")
    email = app.ses.get_email(result["message_id"])
    assert email["subject"] == "find me"


def test_get_email_unknown_raises(app):
    with pytest.raises(NotFound):
        app.ses.get_email("no-such-id@openaws.local")


def test_delete_emails(app):
    app.ses.send_email("s@e.com", ["r@e.com"], "x", body_text="x")
    app.ses.send_email("s@e.com", ["r@e.com"], "y", body_text="y")
    count = app.ses.delete_emails()
    assert count == 2
    assert app.ses.list_emails() == []


# ---------------------------------------------------------------------------
# HTTP server round-trip
# ---------------------------------------------------------------------------

def test_ses_via_http(server):
    import urllib.request

    base = server.base_url

    def post(payload):
        data = json.dumps(payload).encode()
        req = urllib.request.Request(
            f"{base}/ses",
            data=data,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read())

    post({"action": "verify_email_identity", "email": "sender@test.com"})
    assert "sender@test.com" in post({"action": "list_identities"})["identities"]

    result = post({
        "action": "send_email",
        "source": "sender@test.com",
        "to_addresses": ["recv@test.com"],
        "subject": "Test subject",
        "body_text": "Test body",
    })
    assert result["message_id"].endswith("@openaws.local")

    emails = post({"action": "list_emails"})["emails"]
    assert len(emails) == 1
    assert emails[0]["subject"] == "Test subject"

    msg_id = emails[0]["message_id"]
    got = post({"action": "get_email", "msg_id": msg_id})
    assert got["subject"] == "Test subject"

    post({"action": "delete_emails"})
    assert post({"action": "list_emails"})["emails"] == []

    post({"action": "delete_identity", "email": "sender@test.com"})
    assert post({"action": "list_identities"})["identities"] == []
