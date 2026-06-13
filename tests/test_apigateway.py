"""Tests for API Gateway: REST APIs, resources/methods, Lambda integration, invocation."""

import json

import pytest

from openaws.errors import Conflict, NotFound, ValidationError

ECHO_SOURCE = """
def handler(event, context):
    return {
        "statusCode": 200,
        "body": json.dumps({"path": event["path"], "method": event["httpMethod"],
                            "body": event.get("body")}),
        "headers": {"Content-Type": "application/json"},
    }
"""

IMPORT_SOURCE = """
import json
def handler(event, context):
    return {
        "statusCode": 200,
        "body": json.dumps({"path": event["path"], "method": event["httpMethod"],
                            "body": event.get("body")}),
        "headers": {"Content-Type": "application/json"},
    }
"""


# ---------------------------------------------------------------------------
# REST API CRUD
# ---------------------------------------------------------------------------

def test_create_list_delete_api(app):
    api = app.apigateway.create_rest_api("my-api", "My test API")
    assert api["name"] == "my-api"
    assert api["id"]
    apis = app.apigateway.list_rest_apis()
    assert any(a["name"] == "my-api" for a in apis)
    app.apigateway.delete_rest_api(api["id"])
    assert all(a["name"] != "my-api" for a in app.apigateway.list_rest_apis())


def test_duplicate_api_raises(app):
    app.apigateway.create_rest_api("dup")
    with pytest.raises(Conflict):
        app.apigateway.create_rest_api("dup")


def test_delete_unknown_api_raises(app):
    with pytest.raises(NotFound):
        app.apigateway.delete_rest_api("nope")


# ---------------------------------------------------------------------------
# Resources
# ---------------------------------------------------------------------------

def test_create_and_list_resources(app):
    api = app.apigateway.create_rest_api("api")
    r = app.apigateway.create_resource(api["id"], "/users", "GET",
                                        integration_type="mock")
    assert r["path"] == "/users"
    assert r["http_method"] == "GET"
    resources = app.apigateway.list_resources(api["id"])
    assert len(resources) == 1


def test_resource_upsert(app):
    api = app.apigateway.create_rest_api("api")
    r1 = app.apigateway.create_resource(api["id"], "/x", "POST",
                                         integration_uri="fn-a")
    r2 = app.apigateway.create_resource(api["id"], "/x", "POST",
                                         integration_uri="fn-b")
    # same id, updated uri
    assert r1["id"] == r2["id"]
    resources = app.apigateway.list_resources(api["id"])
    assert len(resources) == 1
    assert resources[0]["integration_uri"] == "fn-b"


def test_delete_resource(app):
    api = app.apigateway.create_rest_api("api")
    r = app.apigateway.create_resource(api["id"], "/x", "GET",
                                        integration_type="mock")
    app.apigateway.delete_resource(api["id"], r["id"])
    assert app.apigateway.list_resources(api["id"]) == []


def test_bad_integration_type_raises(app):
    api = app.apigateway.create_rest_api("api")
    with pytest.raises(ValidationError):
        app.apigateway.create_resource(api["id"], "/x", "GET", integration_type="http")


# ---------------------------------------------------------------------------
# Mock integration
# ---------------------------------------------------------------------------

def test_mock_integration_invoke(app):
    api = app.apigateway.create_rest_api("api")
    app.apigateway.create_resource(api["id"], "/ping", "GET", integration_type="mock")
    result = app.apigateway.invoke(api["id"], "GET", "/ping")
    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    assert body["message"] == "mock response"


# ---------------------------------------------------------------------------
# Lambda integration
# ---------------------------------------------------------------------------

def test_lambda_integration_invoke(app):
    app.lambdas.register_source("echo-fn", IMPORT_SOURCE)
    api = app.apigateway.create_rest_api("api")
    app.apigateway.create_resource(api["id"], "/hello", "POST",
                                    integration_type="lambda",
                                    integration_uri="arn:aws:lambda:local:000:function/echo-fn")
    result = app.apigateway.invoke(api["id"], "POST", "/hello", body='{"key":"val"}')
    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    assert body["path"] == "/hello"
    assert body["method"] == "POST"


def test_lambda_result_passthrough(app):
    """If Lambda returns a dict without statusCode, wrap it."""
    app.lambdas.register_callable("simple", lambda e, c: {"echo": e["path"]})
    api = app.apigateway.create_rest_api("api")
    app.apigateway.create_resource(api["id"], "/x", "GET",
                                    integration_type="lambda",
                                    integration_uri="simple")
    result = app.apigateway.invoke(api["id"], "GET", "/x")
    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    assert body["echo"] == "/x"


def test_path_params_routing(app):
    """Test /users/{id} template matching."""
    received = {}
    def handler(event, context):
        received.update(event.get("pathParameters") or {})
        return {"statusCode": 200, "body": "ok", "headers": {}}

    app.lambdas.register_callable("get-user", handler)
    api = app.apigateway.create_rest_api("api")
    app.apigateway.create_resource(api["id"], "/users/{id}", "GET",
                                    integration_type="lambda",
                                    integration_uri="get-user")
    result = app.apigateway.invoke(api["id"], "GET", "/users/42")
    assert result["statusCode"] == 200
    assert received["id"] == "42"


def test_invoke_missing_route_raises(app):
    api = app.apigateway.create_rest_api("api")
    with pytest.raises(NotFound):
        app.apigateway.invoke(api["id"], "GET", "/nope")


# ---------------------------------------------------------------------------
# HTTP server round-trip (management + /apigw/ invocation)
# ---------------------------------------------------------------------------

def test_apigateway_via_http(server):
    import urllib.request

    base = server.base_url

    def post_mgmt(payload):
        data = json.dumps(payload).encode()
        req = urllib.request.Request(
            f"{base}/apigateway",
            data=data,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read())

    # register lambda function
    data = json.dumps({"action": "register_source", "name": "ping-fn",
                       "source": IMPORT_SOURCE}).encode()
    req = urllib.request.Request(f"{base}/lambda", data=data,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req) as r:
        json.loads(r.read())

    api = post_mgmt({"action": "create_rest_api", "name": "test-api"})
    api_id = api["id"]

    post_mgmt({"action": "create_resource", "api_id": api_id,
               "path": "/ping", "http_method": "GET",
               "integration_type": "lambda", "integration_uri": "ping-fn"})

    # invoke via /apigw/<api_id>/ping
    req2 = urllib.request.Request(f"{base}/apigw/{api_id}/ping")
    with urllib.request.urlopen(req2) as r:
        resp = json.loads(r.read())
    assert resp["path"] == "/ping"
    assert resp["method"] == "GET"

    # list
    apis = post_mgmt({"action": "list_rest_apis"})
    assert any(a["name"] == "test-api" for a in apis["apis"])
