"""End-to-end tests that drive a real HTTP server over the loopback socket."""

import json
import urllib.request
import urllib.error


def _request(url, method="GET", data=None, headers=None):
    req = urllib.request.Request(url, data=data, method=method, headers=headers or {})
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, resp.read(), dict(resp.headers)
    except urllib.error.HTTPError as e:
        return e.code, e.read(), dict(e.headers)


def _json(url, method="GET", payload=None):
    data = json.dumps(payload).encode() if payload is not None else None
    headers = {"Content-Type": "application/json"} if data else {}
    status, body, _ = _request(url, method, data, headers)
    return status, json.loads(body) if body else None


def test_health(server):
    status, body = _json(server.base_url + "/")
    assert status == 200
    assert body["service"] == "openaws"
    assert set(body["services"]) == {"s3", "dynamodb", "sqs", "lambda"}


def test_s3_http_round_trip(server):
    base = server.base_url
    status, _ = _json(base + "/s3/web", method="PUT")
    assert status == 201
    # put object (raw bytes)
    status, _, _ = _request(
        base + "/s3/web/index.html", "PUT", b"<h1>hi</h1>", {"Content-Type": "text/html"}
    )
    assert status == 201
    # get object back
    status, body, hdrs = _request(base + "/s3/web/index.html", "GET")
    assert status == 200
    assert body == b"<h1>hi</h1>"
    assert hdrs.get("Content-Type") == "text/html"
    # list objects
    status, listing = _json(base + "/s3/web")
    assert [o["key"] for o in listing["objects"]] == ["index.html"]
    # list buckets
    status, buckets = _json(base + "/s3")
    assert any(b["name"] == "web" for b in buckets["buckets"])
    # delete
    status, _ = _json(base + "/s3/web/index.html", method="DELETE")
    assert status == 200


def test_s3_http_404(server):
    status, body = _json(server.base_url + "/s3/missing/key", method="GET")
    assert status == 404
    assert body["error"] == "NotFound"


def test_dynamodb_http(server):
    url = server.base_url + "/dynamodb"
    assert _json(url, "POST", {"action": "create_table", "name": "u", "hash_key": "id"})[0] == 200
    _json(url, "POST", {"action": "put_item", "table": "u", "item": {"id": "1", "n": "a"}})
    status, got = _json(url, "POST", {"action": "get_item", "table": "u", "key": {"id": "1"}})
    assert status == 200
    assert got["item"]["n"] == "a"
    status, scan = _json(url, "POST", {"action": "scan", "table": "u"})
    assert len(scan["items"]) == 1


def test_sqs_http(server):
    url = server.base_url + "/sqs"
    _json(url, "POST", {"action": "create_queue", "name": "q"})
    _json(url, "POST", {"action": "send_message", "queue": "q", "body": "hello"})
    status, recv = _json(url, "POST", {"action": "receive_messages", "queue": "q"})
    assert status == 200
    assert recv["messages"][0]["body"] == "hello"
    handle = recv["messages"][0]["receipt_handle"]
    status, dele = _json(url, "POST", {"action": "delete_message", "queue": "q", "receipt_handle": handle})
    assert dele["deleted"] is True


def test_lambda_http(server):
    url = server.base_url + "/lambda"
    src = "def handler(event, context):\n    return event['a'] * 2\n"
    assert _json(url, "POST", {"action": "register_source", "name": "dbl", "source": src})[0] == 200
    status, out = _json(url, "POST", {"action": "invoke", "name": "dbl", "event": {"a": 21}})
    assert status == 200
    assert out["result"] == 42


def test_lambda_sqs_integration_http(server):
    """Full cross-service flow over HTTP: SQS -> Lambda."""
    base = server.base_url
    _json(base + "/sqs", "POST", {"action": "create_queue", "name": "work"})
    _json(base + "/sqs", "POST", {"action": "send_message", "queue": "work", "body": "job"})
    src = (
        "def handler(event, context):\n"
        "    return [r['body'].upper() for r in event['Records']]\n"
    )
    _json(base + "/lambda", "POST", {"action": "register_source", "name": "up", "source": src})
    status, out = _json(
        base + "/lambda", "POST", {"action": "invoke_from_sqs", "name": "up", "queue": "work"}
    )
    assert status == 200
    assert out["results"] == [["JOB"]]
    # message consumed
    status, cnt = _json(base + "/sqs", "POST", {"action": "message_count", "queue": "work"})
    assert cnt["count"] == 0


def test_bad_action_returns_400(server):
    status, body = _json(server.base_url + "/sqs", "POST", {"action": "nope"})
    assert status == 400
    assert body["error"] == "ValidationException"
