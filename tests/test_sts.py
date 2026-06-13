"""Tests for STSService — AssumeRole, GetCallerIdentity, GetSessionToken."""
import pytest

from openaws.errors import NotFound, ValidationError


def test_get_caller_identity_default(app):
    identity = app.sts.get_caller_identity()
    assert identity["account"] == "000000000000"
    assert "arn" in identity


def test_get_session_token(app):
    result = app.sts.get_session_token()
    creds = result["credentials"]
    assert creds["access_key_id"].startswith("ASIA")
    assert len(creds["secret_access_key"]) >= 32
    assert "session_token" in creds
    assert creds["expiration"] > 0


def test_assume_role(app):
    trust = {"Statement": [{"Effect": "Allow", "Principal": {"Service": "ec2.amazonaws.com"}, "Action": "sts:AssumeRole"}]}
    app.iam.create_role("test-role", trust)
    result = app.sts.assume_role("test-role", "my-session")
    creds = result["credentials"]
    assert creds["access_key_id"].startswith("ASIA")
    assert "assumed_role_user" in result
    assert "test-role" in result["assumed_role_user"]["arn"]


def test_assume_role_invalid_role(app):
    with pytest.raises(NotFound):
        app.sts.assume_role("arn:openaws:iam::000:role/nonexistent", "sess")


def test_assume_role_requires_role_arn(app):
    with pytest.raises(ValidationError):
        app.sts.assume_role("", "sess")


def test_assume_role_requires_session_name(app):
    trust = {"Statement": []}
    app.iam.create_role("role2", trust)
    with pytest.raises(ValidationError):
        app.sts.assume_role("role2", "")


def test_get_caller_identity_with_session_creds(app):
    trust = {"Statement": []}
    app.iam.create_role("role3", trust)
    result = app.sts.assume_role("role3", "s1")
    key_id = result["credentials"]["access_key_id"]
    identity = app.sts.get_caller_identity(access_key_id=key_id)
    assert identity["account"] == "000000000000"
    assert "role3" in identity["arn"]


def test_get_caller_identity_with_user_access_key(app):
    app.iam.create_user("sts-user")
    key = app.iam.create_access_key("sts-user")
    identity = app.sts.get_caller_identity(access_key_id=key["access_key_id"])
    assert "sts-user" in identity["arn"]


def test_list_and_revoke_sessions(app):
    trust = {"Statement": []}
    app.iam.create_role("role4", trust)
    result = app.sts.assume_role("role4", "s1")
    key_id = result["credentials"]["access_key_id"]
    sessions = app.sts.list_sessions()
    assert any(s["access_key_id"] == key_id for s in sessions)
    app.sts.revoke_session(key_id)
    sessions_after = app.sts.list_sessions()
    assert not any(s["access_key_id"] == key_id for s in sessions_after)


def test_sts_http_roundtrip(server):
    import urllib.request, json
    base = server.base_url + "/sts"

    def call(payload):
        data = json.dumps(payload).encode()
        req = urllib.request.Request(base, data=data,
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read())

    resp = call({"action": "get_caller_identity"})
    assert resp["account"] == "000000000000"

    resp = call({"action": "get_session_token"})
    assert "credentials" in resp


def test_sts_assume_role_http(server):
    import urllib.request, json

    def iam_call(payload):
        data = json.dumps(payload).encode()
        req = urllib.request.Request(server.base_url + "/iam", data=data,
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read())

    def sts_call(payload):
        data = json.dumps(payload).encode()
        req = urllib.request.Request(server.base_url + "/sts", data=data,
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read())

    trust = {"Statement": []}
    iam_call({"action": "create_role", "role_name": "http-role", "assume_role_policy": trust})
    resp = sts_call({"action": "assume_role", "role_arn": "http-role", "role_session_name": "sess"})
    assert resp["credentials"]["access_key_id"].startswith("ASIA")
