"""Tests for SSM Parameter Store."""
import pytest

from openaws.errors import Conflict, NotFound, ValidationError


def test_put_and_get_parameter(app):
    app.ssm.put_parameter("/app/db/host", "localhost")
    p = app.ssm.get_parameter("/app/db/host")
    assert p["value"] == "localhost"
    assert p["type"] == "String"
    assert p["version"] == 1


def test_put_stringlist(app):
    app.ssm.put_parameter("/app/allowed_regions", "us-east-1,us-west-2", param_type="StringList")
    p = app.ssm.get_parameter("/app/allowed_regions")
    assert p["type"] == "StringList"
    assert "us-east-1" in p["value"]


def test_duplicate_raises_without_overwrite(app):
    app.ssm.put_parameter("/x", "v1")
    with pytest.raises(Conflict):
        app.ssm.put_parameter("/x", "v2")


def test_overwrite_increments_version(app):
    app.ssm.put_parameter("/counter", "1")
    app.ssm.put_parameter("/counter", "2", overwrite=True)
    p = app.ssm.get_parameter("/counter")
    assert p["value"] == "2"
    assert p["version"] == 2


def test_get_parameters_multiple(app):
    app.ssm.put_parameter("/a", "1")
    app.ssm.put_parameter("/b", "2")
    result = app.ssm.get_parameters(["/a", "/b", "/missing"])
    assert len(result["parameters"]) == 2
    assert "/missing" in result["invalid_parameters"]


def test_get_parameters_by_path(app):
    app.ssm.put_parameter("/svc/db/host", "h")
    app.ssm.put_parameter("/svc/db/port", "5432")
    app.ssm.put_parameter("/svc/cache/host", "redis")
    # non-recursive: only /svc/db/* direct children
    params = app.ssm.get_parameters_by_path("/svc/db")
    names = [p["name"] for p in params]
    assert "/svc/db/host" in names
    assert "/svc/db/port" in names
    assert "/svc/cache/host" not in names


def test_get_parameters_by_path_recursive(app):
    app.ssm.put_parameter("/tree/a/b/c", "v")
    app.ssm.put_parameter("/tree/a/d", "v2")
    params = app.ssm.get_parameters_by_path("/tree", recursive=True)
    names = [p["name"] for p in params]
    assert "/tree/a/b/c" in names
    assert "/tree/a/d" in names


def test_delete_parameter(app):
    app.ssm.put_parameter("/del-me", "x")
    app.ssm.delete_parameter("/del-me")
    with pytest.raises(NotFound):
        app.ssm.get_parameter("/del-me")


def test_delete_parameters_batch(app):
    app.ssm.put_parameter("/d1", "a")
    app.ssm.put_parameter("/d2", "b")
    result = app.ssm.delete_parameters(["/d1", "/d2", "/missing"])
    assert "/d1" in result["deleted"]
    assert "/d2" in result["deleted"]
    assert "/missing" in result["invalid"]


def test_describe_parameters(app):
    app.ssm.put_parameter("/desc/one", "1")
    app.ssm.put_parameter("/desc/two", "2")
    params = app.ssm.describe_parameters()
    names = [p["name"] for p in params]
    assert "/desc/one" in names


def test_describe_parameters_filter(app):
    app.ssm.put_parameter("/filter/alpha", "a")
    app.ssm.put_parameter("/filter/beta", "b")
    params = app.ssm.describe_parameters(
        filters=[{"Key": "Name", "Option": "BeginsWith", "Values": ["/filter/"]}]
    )
    names = [p["name"] for p in params]
    assert "/filter/alpha" in names and "/filter/beta" in names


def test_parameter_history(app):
    app.ssm.put_parameter("/hist", "v1")
    app.ssm.put_parameter("/hist", "v2", overwrite=True)
    app.ssm.put_parameter("/hist", "v3", overwrite=True)
    history = app.ssm.list_parameter_history("/hist")
    assert len(history) == 3
    versions = [h["version"] for h in history]
    assert versions == [1, 2, 3]


def test_invalid_type_raises(app):
    with pytest.raises(ValidationError):
        app.ssm.put_parameter("/bad", "x", param_type="InvalidType")


def test_ssm_http_roundtrip(server):
    import urllib.request, json
    base = server.base_url + "/ssm"

    def call(payload):
        data = json.dumps(payload).encode()
        req = urllib.request.Request(base, data=data,
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read())

    call({"action": "put_parameter", "name": "/http/param", "value": "42"})
    resp = call({"action": "get_parameter", "name": "/http/param"})
    assert resp["parameter"]["value"] == "42"
    resp2 = call({"action": "describe_parameters"})
    names = [p["name"] for p in resp2["parameters"]]
    assert "/http/param" in names


def test_ssm_secure_string_with_kms(app):
    """SecureString: encrypted at rest, decrypted transparently on get."""
    key = app.kms.create_key()
    app.ssm.put_parameter(
        "/secure/token",
        "my-api-key",
        param_type="SecureString",
        kms_key_id=key["key_id"],
    )
    # with_decryption=True (default) should return plaintext
    p = app.ssm.get_parameter("/secure/token", with_decryption=True)
    assert p["value"] == "my-api-key"
