"""Tests for IAMService — users, groups, roles, policies, simulate."""
import pytest

from openaws.errors import Conflict, NotFound, ValidationError


# ---------------------------------------------------------------------------
# Users
# ---------------------------------------------------------------------------

def test_create_get_delete_user(app):
    u = app.iam.create_user("alice")
    assert u["username"] == "alice"
    assert u["arn"].startswith("arn:openaws:iam")
    got = app.iam.get_user("alice")
    assert got["username"] == "alice"
    app.iam.delete_user("alice")
    with pytest.raises(NotFound):
        app.iam.get_user("alice")


def test_create_duplicate_user_raises(app):
    app.iam.create_user("bob")
    with pytest.raises(Conflict):
        app.iam.create_user("bob")


def test_list_users(app):
    app.iam.create_user("u1")
    app.iam.create_user("u2")
    names = [u["username"] for u in app.iam.list_users()]
    assert "u1" in names and "u2" in names


def test_user_requires_name(app):
    with pytest.raises(ValidationError):
        app.iam.create_user("")


# ---------------------------------------------------------------------------
# Access Keys
# ---------------------------------------------------------------------------

def test_create_list_delete_access_key(app):
    app.iam.create_user("carol")
    key = app.iam.create_access_key("carol")
    assert key["access_key_id"].startswith("AKIA")
    assert len(key["secret_access_key"]) >= 32
    keys = app.iam.list_access_keys("carol")
    assert any(k["key_id"] == key["access_key_id"] for k in keys)
    app.iam.delete_access_key(key["access_key_id"])
    assert app.iam.list_access_keys("carol") == []


def test_access_key_for_nonexistent_user(app):
    with pytest.raises(NotFound):
        app.iam.create_access_key("nobody")


# ---------------------------------------------------------------------------
# Groups
# ---------------------------------------------------------------------------

def test_create_list_delete_group(app):
    app.iam.create_group("admins")
    groups = [g["group_name"] for g in app.iam.list_groups()]
    assert "admins" in groups
    app.iam.delete_group("admins")
    assert "admins" not in [g["group_name"] for g in app.iam.list_groups()]


def test_add_remove_user_from_group(app):
    app.iam.create_user("dave")
    app.iam.create_group("devs")
    app.iam.add_user_to_group("dave", "devs")
    assert "devs" in app.iam.list_groups_for_user("dave")
    app.iam.remove_user_from_group("dave", "devs")
    assert "devs" not in app.iam.list_groups_for_user("dave")


def test_add_user_to_group_idempotent(app):
    app.iam.create_user("eve")
    app.iam.create_group("ops")
    app.iam.add_user_to_group("eve", "ops")
    app.iam.add_user_to_group("eve", "ops")  # should not raise
    assert app.iam.list_groups_for_user("eve") == ["ops"]


# ---------------------------------------------------------------------------
# Roles
# ---------------------------------------------------------------------------

def test_create_get_delete_role(app):
    trust = {"Statement": [{"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}]}
    r = app.iam.create_role("my-role", trust, description="test role")
    assert r["role_name"] == "my-role"
    got = app.iam.get_role("my-role")
    assert got["assume_role_policy"]["Statement"][0]["Effect"] == "Allow"
    app.iam.delete_role("my-role")
    with pytest.raises(NotFound):
        app.iam.get_role("my-role")


def test_list_roles(app):
    trust = {"Statement": []}
    app.iam.create_role("r1", trust)
    app.iam.create_role("r2", trust)
    names = [r["role_name"] for r in app.iam.list_roles()]
    assert "r1" in names and "r2" in names


# ---------------------------------------------------------------------------
# Managed policies
# ---------------------------------------------------------------------------

def test_create_get_delete_policy(app):
    doc = {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}]}
    p = app.iam.create_policy("s3-full", doc)
    assert p["policy_name"] == "s3-full"
    got = app.iam.get_policy("s3-full")
    assert got["document"]["Statement"][0]["Effect"] == "Allow"
    app.iam.delete_policy("s3-full")
    with pytest.raises(NotFound):
        app.iam.get_policy("s3-full")


def test_list_policies(app):
    doc = {"Statement": []}
    app.iam.create_policy("p1", doc)
    app.iam.create_policy("p2", doc)
    names = [p["policy_name"] for p in app.iam.list_policies()]
    assert "p1" in names and "p2" in names


# ---------------------------------------------------------------------------
# Inline policies
# ---------------------------------------------------------------------------

def test_put_get_delete_inline_policy_user(app):
    app.iam.create_user("frank")
    doc = {"Statement": [{"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"}]}
    app.iam.put_inline_policy("user", "frank", "read-s3", doc)
    names = app.iam.list_inline_policies("user", "frank")
    assert "read-s3" in names
    got = app.iam.get_inline_policy("user", "frank", "read-s3")
    assert got["document"]["Statement"][0]["Action"] == "s3:GetObject"
    app.iam.delete_inline_policy("user", "frank", "read-s3")
    assert app.iam.list_inline_policies("user", "frank") == []


def test_put_inline_policy_upsert(app):
    app.iam.create_user("grace")
    doc1 = {"Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}]}
    doc2 = {"Statement": [{"Effect": "Deny", "Action": "s3:DeleteObject", "Resource": "*"}]}
    app.iam.put_inline_policy("user", "grace", "s3-pol", doc1)
    app.iam.put_inline_policy("user", "grace", "s3-pol", doc2)  # upsert
    got = app.iam.get_inline_policy("user", "grace", "s3-pol")
    assert got["document"]["Statement"][0]["Effect"] == "Deny"


# ---------------------------------------------------------------------------
# Attach / detach policies
# ---------------------------------------------------------------------------

def test_attach_detach_policy_user(app):
    app.iam.create_user("henry")
    doc = {"Statement": [{"Effect": "Allow", "Action": "ec2:*", "Resource": "*"}]}
    app.iam.create_policy("ec2-full", doc)
    app.iam.attach_policy("user", "henry", "ec2-full")
    assert "ec2-full" in app.iam.list_attached_policies("user", "henry")
    app.iam.detach_policy("user", "henry", "ec2-full")
    assert "ec2-full" not in app.iam.list_attached_policies("user", "henry")


def test_attach_policy_idempotent(app):
    app.iam.create_user("ida")
    doc = {"Statement": []}
    app.iam.create_policy("noop", doc)
    app.iam.attach_policy("user", "ida", "noop")
    app.iam.attach_policy("user", "ida", "noop")  # should not raise
    assert app.iam.list_attached_policies("user", "ida") == ["noop"]


# ---------------------------------------------------------------------------
# simulate_principal_policy
# ---------------------------------------------------------------------------

def test_simulate_allow(app):
    app.iam.create_user("jim")
    doc = {"Statement": [{"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"}]}
    app.iam.put_inline_policy("user", "jim", "pol", doc)
    results = app.iam.simulate_principal_policy("user", "jim", ["s3:GetObject"])
    assert results[0]["eval_decision"] == "allowed"


def test_simulate_implicit_deny(app):
    app.iam.create_user("kim")
    doc = {"Statement": [{"Effect": "Allow", "Action": "s3:PutObject", "Resource": "*"}]}
    app.iam.put_inline_policy("user", "kim", "pol", doc)
    results = app.iam.simulate_principal_policy("user", "kim", ["s3:GetObject"])
    assert results[0]["eval_decision"] == "implicitDeny"


def test_simulate_explicit_deny_overrides_allow(app):
    app.iam.create_user("leo")
    allow_doc = {"Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}]}
    deny_doc = {"Statement": [{"Effect": "Deny", "Action": "s3:DeleteObject", "Resource": "*"}]}
    app.iam.put_inline_policy("user", "leo", "allow-pol", allow_doc)
    app.iam.put_inline_policy("user", "leo", "deny-pol", deny_doc)
    results = app.iam.simulate_principal_policy("user", "leo", ["s3:DeleteObject"])
    assert results[0]["eval_decision"] == "explicitDeny"


def test_simulate_wildcard_action(app):
    app.iam.create_user("mia")
    doc = {"Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}]}
    app.iam.put_inline_policy("user", "mia", "pol", doc)
    results = app.iam.simulate_principal_policy("user", "mia", ["s3:ListBuckets"])
    assert results[0]["eval_decision"] == "allowed"


def test_simulate_via_attached_managed_policy(app):
    app.iam.create_user("ned")
    doc = {"Statement": [{"Effect": "Allow", "Action": "dynamodb:*", "Resource": "*"}]}
    app.iam.create_policy("ddb-full", doc)
    app.iam.attach_policy("user", "ned", "ddb-full")
    results = app.iam.simulate_principal_policy("user", "ned", ["dynamodb:PutItem"])
    assert results[0]["eval_decision"] == "allowed"


# ---------------------------------------------------------------------------
# HTTP round-trip
# ---------------------------------------------------------------------------

def test_iam_http_roundtrip(server):
    import urllib.request, json
    base = server.base_url + "/iam"

    def call(payload):
        data = json.dumps(payload).encode()
        req = urllib.request.Request(base, data=data,
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read())

    call({"action": "create_user", "username": "http-user"})
    resp = call({"action": "list_users"})
    names = [u["username"] for u in resp["users"]]
    assert "http-user" in names
    call({"action": "delete_user", "username": "http-user"})
