"""Tests for SecretsManagerService."""
import pytest

from openaws.errors import Conflict, NotFound, ValidationError


def test_create_and_describe_secret(app):
    s = app.secretsmanager.create_secret("my/secret", secret_string='{"key":"value"}')
    assert s["name"] == "my/secret"
    assert "version_id" in s
    desc = app.secretsmanager.describe_secret("my/secret")
    assert desc["name"] == "my/secret"


def test_duplicate_secret_raises(app):
    app.secretsmanager.create_secret("dup", secret_string="v1")
    with pytest.raises(Conflict):
        app.secretsmanager.create_secret("dup", secret_string="v2")


def test_create_secret_requires_value(app):
    with pytest.raises(ValidationError):
        app.secretsmanager.create_secret("empty-secret")


def test_list_secrets(app):
    app.secretsmanager.create_secret("s1", secret_string="a")
    app.secretsmanager.create_secret("s2", secret_string="b")
    names = [s["name"] for s in app.secretsmanager.list_secrets()]
    assert "s1" in names and "s2" in names


def test_get_secret_value_current(app):
    app.secretsmanager.create_secret("pw", secret_string="supersecret")
    val = app.secretsmanager.get_secret_value("pw")
    assert val["secret_string"] == "supersecret"
    assert "AWSCURRENT" in val["version_stages"]


def test_put_secret_value_version_rotation(app):
    app.secretsmanager.create_secret("ver-sec", secret_string="v1")
    v1 = app.secretsmanager.get_secret_value("ver-sec")
    app.secretsmanager.put_secret_value("ver-sec", secret_string="v2")
    v_current = app.secretsmanager.get_secret_value("ver-sec")
    assert v_current["secret_string"] == "v2"
    # old version becomes AWSPREVIOUS
    old = app.secretsmanager.get_secret_value("ver-sec", version_stage="AWSPREVIOUS")
    assert old["secret_string"] == "v1"


def test_get_by_version_id(app):
    app.secretsmanager.create_secret("vid-sec", secret_string="original")
    v1 = app.secretsmanager.get_secret_value("vid-sec")
    v1_id = v1["version_id"]
    app.secretsmanager.put_secret_value("vid-sec", secret_string="updated")
    retrieved = app.secretsmanager.get_secret_value("vid-sec", version_id=v1_id)
    assert retrieved["secret_string"] == "original"


def test_update_secret_description(app):
    app.secretsmanager.create_secret("upd", secret_string="x")
    app.secretsmanager.update_secret("upd", description="new desc")
    desc = app.secretsmanager.describe_secret("upd")
    assert desc["description"] == "new desc"


def test_delete_secret_soft(app):
    app.secretsmanager.create_secret("soft-del", secret_string="x")
    result = app.secretsmanager.delete_secret("soft-del", recovery_window_in_days=7)
    assert "deletion_date" in result
    # still retrievable in list
    secrets = app.secretsmanager.list_secrets()
    assert any(s["name"] == "soft-del" for s in secrets)


def test_delete_secret_force(app):
    app.secretsmanager.create_secret("hard-del", secret_string="x")
    app.secretsmanager.delete_secret("hard-del", force_delete_without_recovery=True)
    with pytest.raises(NotFound):
        app.secretsmanager.describe_secret("hard-del")


def test_restore_secret(app):
    app.secretsmanager.create_secret("restore-me", secret_string="x")
    app.secretsmanager.delete_secret("restore-me")
    app.secretsmanager.restore_secret("restore-me")
    desc = app.secretsmanager.describe_secret("restore-me")
    assert desc["deleted_at"] is None


def test_list_version_ids(app):
    app.secretsmanager.create_secret("multi-ver", secret_string="v1")
    app.secretsmanager.put_secret_value("multi-ver", secret_string="v2")
    versions = app.secretsmanager.list_secret_version_ids("multi-ver")
    assert len(versions) == 2


def test_rotate_secret_stub(app):
    app.secretsmanager.create_secret("rot-sec", secret_string="x")
    result = app.secretsmanager.rotate_secret("rot-sec", "arn:lambda:rotation", {"automatically_after_days": 30})
    assert result["rotation_enabled"] is True


def test_tags(app):
    app.secretsmanager.create_secret("tagged", secret_string="x")
    app.secretsmanager.tag_resource("tagged", [{"Key": "env", "Value": "prod"}])
    tags = app.secretsmanager.list_tags_for_resource("tagged")
    assert any(t["Key"] == "env" for t in tags)
    app.secretsmanager.untag_resource("tagged", ["env"])
    assert app.secretsmanager.list_tags_for_resource("tagged") == []


def test_secretsmanager_http_roundtrip(server):
    import urllib.request, json
    base = server.base_url + "/secretsmanager"

    def call(payload):
        data = json.dumps(payload).encode()
        req = urllib.request.Request(base, data=data,
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read())

    call({"action": "create_secret", "name": "http-secret", "secret_string": "my-password"})
    resp = call({"action": "get_secret_value", "name": "http-secret"})
    assert resp["secret_string"] == "my-password"
    secrets = call({"action": "list_secrets"})
    assert any(s["name"] == "http-secret" for s in secrets["secrets"])
