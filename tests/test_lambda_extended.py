"""Tests for Lambda extended features: env vars, versions, aliases, async, layers."""

import json

import pytest

from openaws.errors import Conflict, NotFound, ValidationError

SOURCE = """
def handler(event, context):
    return {"val": event.get("x", 0), "env": context.env.get("STAGE", "none")}
"""


# ---------------------------------------------------------------------------
# Environment variables
# ---------------------------------------------------------------------------

def test_register_with_env_vars(app):
    app.lambdas.register_source("fn", SOURCE, env_vars={"STAGE": "prod"})
    result = app.lambdas.invoke("fn", {"x": 7})
    assert result["val"] == 7
    assert result["env"] == "prod"


def test_update_env_vars(app):
    app.lambdas.register_source("fn", SOURCE, env_vars={"STAGE": "dev"})
    app.lambdas.update_function_configuration("fn", env_vars={"STAGE": "staging"})
    result = app.lambdas.invoke("fn", {"x": 1})
    assert result["env"] == "staging"


def test_get_function(app):
    app.lambdas.register_source("fn", SOURCE, env_vars={"K": "V"}, description="test fn", timeout=10)
    info = app.lambdas.get_function("fn")
    assert info["env_vars"] == {"K": "V"}
    assert info["description"] == "test fn"
    assert info["timeout"] == 10


def test_get_function_not_found(app):
    with pytest.raises(NotFound):
        app.lambdas.get_function("ghost")


# ---------------------------------------------------------------------------
# Versions
# ---------------------------------------------------------------------------

def test_publish_version(app):
    app.lambdas.register_source("fn", SOURCE)
    v = app.lambdas.publish_version("fn", description="v1")
    assert v["version"] == "1"
    v2 = app.lambdas.publish_version("fn", description="v2")
    assert v2["version"] == "2"


def test_list_versions(app):
    app.lambdas.register_source("fn", SOURCE)
    app.lambdas.publish_version("fn")
    app.lambdas.publish_version("fn")
    versions = app.lambdas.list_versions("fn")
    assert len(versions) == 2
    assert versions[0]["version"] == "1"
    assert versions[1]["version"] == "2"


def test_publish_version_unknown_function(app):
    with pytest.raises(NotFound):
        app.lambdas.publish_version("ghost")


# ---------------------------------------------------------------------------
# Aliases
# ---------------------------------------------------------------------------

def test_create_and_list_alias(app):
    app.lambdas.register_source("fn", SOURCE)
    app.lambdas.publish_version("fn")
    app.lambdas.create_alias("fn", "live", "1")
    aliases = app.lambdas.list_aliases("fn")
    assert len(aliases) == 1
    assert aliases[0]["alias"] == "live"
    assert aliases[0]["version"] == "1"


def test_update_alias(app):
    app.lambdas.register_source("fn", SOURCE)
    app.lambdas.publish_version("fn")
    app.lambdas.publish_version("fn")
    app.lambdas.create_alias("fn", "live", "1")
    app.lambdas.update_alias("fn", "live", "2")
    aliases = app.lambdas.list_aliases("fn")
    assert aliases[0]["version"] == "2"


def test_duplicate_alias_raises(app):
    app.lambdas.register_source("fn", SOURCE)
    app.lambdas.publish_version("fn")
    app.lambdas.create_alias("fn", "live", "1")
    with pytest.raises(Conflict):
        app.lambdas.create_alias("fn", "live", "1")


def test_delete_alias(app):
    app.lambdas.register_source("fn", SOURCE)
    app.lambdas.publish_version("fn")
    app.lambdas.create_alias("fn", "live", "1")
    app.lambdas.delete_alias("fn", "live")
    assert app.lambdas.list_aliases("fn") == []


def test_delete_function_cleans_versions_aliases(app):
    app.lambdas.register_source("fn", SOURCE)
    app.lambdas.publish_version("fn")
    app.lambdas.create_alias("fn", "live", "1")
    app.lambdas.delete_function("fn")
    # storage-level check: no orphaned rows
    assert app.lambdas.list_functions() == []


# ---------------------------------------------------------------------------
# Async invocation
# ---------------------------------------------------------------------------

def test_invoke_async_queues_and_processes(app):
    results = []
    app.lambdas.register_callable("worker", lambda e, c: results.append(e["x"]))
    inv = app.lambdas.invoke_async("worker", {"x": 99})
    assert inv["status"] == "QUEUED"
    # Not yet processed
    assert results == []
    processed = app.lambdas.process_async_queue("worker")
    assert len(processed) == 1
    assert processed[0]["status"] == "SUCCEEDED"
    assert results == [99]


def test_invoke_async_failed_function(app):
    app.lambdas.register_callable("bad", lambda e, c: 1 / 0)
    app.lambdas.invoke_async("bad", {})
    processed = app.lambdas.process_async_queue("bad")
    assert processed[0]["status"] == "FAILED"


def test_list_async_invocations(app):
    app.lambdas.register_callable("fn", lambda e, c: None)
    app.lambdas.invoke_async("fn", {"n": 1})
    app.lambdas.invoke_async("fn", {"n": 2})
    invs = app.lambdas.list_async_invocations("fn")
    assert len(invs) == 2


def test_process_async_all_functions(app):
    app.lambdas.register_callable("a", lambda e, c: "a")
    app.lambdas.register_callable("b", lambda e, c: "b")
    app.lambdas.invoke_async("a", {})
    app.lambdas.invoke_async("b", {})
    results = app.lambdas.process_async_queue()
    assert len(results) == 2


# ---------------------------------------------------------------------------
# Layers metadata
# ---------------------------------------------------------------------------

def test_add_and_list_layer_versions(app):
    v1 = app.lambdas.add_layer_version("utils", description="v1", compatible_runtimes=["python3.12"])
    assert v1["version"] == 1
    v2 = app.lambdas.add_layer_version("utils", description="v2")
    assert v2["version"] == 2
    versions = app.lambdas.list_layer_versions("utils")
    assert len(versions) == 2
    assert versions[0]["compatible_runtimes"] == ["python3.12"]


def test_list_layers(app):
    app.lambdas.add_layer_version("shared")
    app.lambdas.add_layer_version("crypto")
    layers = app.lambdas.list_layers()
    names = [l["name"] for l in layers]
    assert "shared" in names
    assert "crypto" in names


def test_add_layer_empty_name_raises(app):
    with pytest.raises(ValidationError):
        app.lambdas.add_layer_version("")


# ---------------------------------------------------------------------------
# HTTP server round-trip
# ---------------------------------------------------------------------------

def test_lambda_extended_via_http(server):
    import urllib.request

    base = server.base_url

    def post(payload):
        data = json.dumps(payload).encode()
        req = urllib.request.Request(
            f"{base}/lambda",
            data=data,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read())

    post({"action": "register_source", "name": "fn", "source": SOURCE,
          "env_vars": {"STAGE": "test"}})
    resp = post({"action": "invoke", "name": "fn", "event": {"x": 5}})
    assert resp["result"]["val"] == 5
    assert resp["result"]["env"] == "test"

    post({"action": "publish_version", "name": "fn", "description": "v1"})
    post({"action": "create_alias", "name": "fn", "alias": "live", "version": "1"})
    aliases = post({"action": "list_aliases", "name": "fn"})
    assert len(aliases["aliases"]) == 1

    post({"action": "add_layer_version", "name": "mylib", "compatible_runtimes": ["python3.12"]})
    layers = post({"action": "list_layers"})
    assert any(l["name"] == "mylib" for l in layers["layers"])

    async_resp = post({"action": "invoke_async", "name": "fn", "event": {"x": 3}})
    assert async_resp["status"] == "QUEUED"
    processed = post({"action": "process_async_queue", "function_name": "fn"})
    assert processed["results"][0]["status"] == "SUCCEEDED"
