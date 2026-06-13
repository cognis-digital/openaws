"""Tests for the extended DynamoDB features:
GSI/LSI, conditional writes, BatchGet/Write, TransactWrite, TTL, UpdateExpression.
"""

import time

import pytest

from openaws.errors import Conflict, NotFound, ValidationError


# ---------------------------------------------------------------------------
# GSI / LSI
# ---------------------------------------------------------------------------

def test_create_table_with_gsi(app):
    app.dynamodb.create_table(
        "orders",
        hash_key="order_id",
        global_secondary_indexes=[
            {"name": "by-customer", "hash_key": "customer_id", "range_key": "created_at"}
        ],
    )
    desc = app.dynamodb.describe_table("orders")
    assert len(desc["global_secondary_indexes"]) == 1
    assert desc["global_secondary_indexes"][0]["name"] == "by-customer"


def test_create_table_with_lsi(app):
    app.dynamodb.create_table(
        "posts",
        hash_key="user_id",
        range_key="post_id",
        local_secondary_indexes=[
            {"name": "by-date", "hash_key": "user_id", "range_key": "created_at"}
        ],
    )
    desc = app.dynamodb.describe_table("posts")
    assert len(desc["local_secondary_indexes"]) == 1


def test_query_gsi(app):
    app.dynamodb.create_table(
        "orders",
        hash_key="order_id",
        global_secondary_indexes=[
            {"name": "by-customer", "hash_key": "customer_id", "range_key": "created_at"}
        ],
    )
    app.dynamodb.put_item("orders", {"order_id": "o1", "customer_id": "c1", "created_at": "2026-01"})
    app.dynamodb.put_item("orders", {"order_id": "o2", "customer_id": "c1", "created_at": "2026-02"})
    app.dynamodb.put_item("orders", {"order_id": "o3", "customer_id": "c2", "created_at": "2026-01"})
    result = app.dynamodb.query("orders", "c1", index_name="by-customer")
    assert len(result) == 2
    assert all(r["customer_id"] == "c1" for r in result)


def test_query_gsi_sort_begins_with(app):
    app.dynamodb.create_table(
        "events",
        hash_key="id",
        global_secondary_indexes=[
            {"name": "by-type", "hash_key": "event_type", "range_key": "ts"}
        ],
    )
    for ev, ts in [("click", "2026-01"), ("click", "2026-02"), ("view", "2026-01")]:
        app.dynamodb.put_item("events", {"id": f"{ev}-{ts}", "event_type": ev, "ts": ts})
    result = app.dynamodb.query("events", "click",
                                sort_begins_with="2026-0", index_name="by-type")
    assert len(result) == 2


def test_query_unknown_index_raises(app):
    app.dynamodb.create_table("t", hash_key="id")
    with pytest.raises(ValidationError, match="no such index"):
        app.dynamodb.query("t", "x", index_name="nonexistent")


# ---------------------------------------------------------------------------
# Conditional writes
# ---------------------------------------------------------------------------

def test_put_condition_attribute_not_exists(app):
    app.dynamodb.create_table("users", "id")
    # first put with condition that 'id' doesn't exist yet — should succeed
    app.dynamodb.put_item(
        "users", {"id": "u1", "name": "Ada"},
        condition={"function": "attribute_not_exists", "attribute": "id"},
    )
    # second put with same condition — should fail because item exists
    with pytest.raises(ValidationError, match="condition failed"):
        app.dynamodb.put_item(
            "users", {"id": "u1", "name": "Bob"},
            condition={"function": "attribute_not_exists", "attribute": "id"},
        )


def test_put_condition_attribute_exists(app):
    app.dynamodb.create_table("users", "id")
    # attribute_exists on non-existent item should fail
    with pytest.raises(ValidationError, match="condition failed"):
        app.dynamodb.put_item(
            "users", {"id": "u1"},
            condition={"function": "attribute_exists", "attribute": "id"},
        )
    # insert the item
    app.dynamodb.put_item("users", {"id": "u1", "active": True})
    # now attribute_exists("id") should succeed
    app.dynamodb.put_item(
        "users", {"id": "u1", "active": False},
        condition={"function": "attribute_exists", "attribute": "id"},
    )
    assert app.dynamodb.get_item("users", {"id": "u1"})["active"] is False


def test_put_condition_attribute_equals(app):
    app.dynamodb.create_table("users", "id")
    app.dynamodb.put_item("users", {"id": "u1", "version": 1})
    # optimistic-lock style: only update if version == 1
    app.dynamodb.put_item(
        "users", {"id": "u1", "version": 2},
        condition={"function": "attribute_equals", "attribute": "version", "value": 1},
    )
    # now version is 2 — condition on version==1 should fail
    with pytest.raises(ValidationError, match="condition failed"):
        app.dynamodb.put_item(
            "users", {"id": "u1", "version": 3},
            condition={"function": "attribute_equals", "attribute": "version", "value": 1},
        )


def test_delete_with_condition(app):
    app.dynamodb.create_table("users", "id")
    app.dynamodb.put_item("users", {"id": "u1", "locked": False})
    # condition: locked == False allows delete
    app.dynamodb.delete_item(
        "users", {"id": "u1"},
        condition={"function": "attribute_equals", "attribute": "locked", "value": False},
    )
    assert app.dynamodb.get_item("users", {"id": "u1"}) is None


def test_condition_unsupported_function_raises(app):
    app.dynamodb.create_table("t", "id")
    with pytest.raises(ValidationError, match="unsupported condition function"):
        app.dynamodb.put_item(
            "t", {"id": "1"},
            condition={"function": "begins_with", "attribute": "id"},
        )


# ---------------------------------------------------------------------------
# UpdateExpression
# ---------------------------------------------------------------------------

def test_update_item_set(app):
    app.dynamodb.create_table("users", "id")
    app.dynamodb.put_item("users", {"id": "u1", "name": "Ada", "score": 10})
    app.dynamodb.update_item("users", {"id": "u1"}, {"SET": {"score": 20, "level": "gold"}})
    item = app.dynamodb.get_item("users", {"id": "u1"})
    assert item["score"] == 20
    assert item["level"] == "gold"
    assert item["name"] == "Ada"  # unchanged


def test_update_item_remove(app):
    app.dynamodb.create_table("users", "id")
    app.dynamodb.put_item("users", {"id": "u1", "name": "Ada", "tmp": "delete-me"})
    app.dynamodb.update_item("users", {"id": "u1"}, {"REMOVE": ["tmp"]})
    item = app.dynamodb.get_item("users", {"id": "u1"})
    assert "tmp" not in item


def test_update_item_add(app):
    app.dynamodb.create_table("counters", "id")
    app.dynamodb.put_item("counters", {"id": "page-hits", "count": 5})
    app.dynamodb.update_item("counters", {"id": "page-hits"}, {"ADD": {"count": 3}})
    assert app.dynamodb.get_item("counters", {"id": "page-hits"})["count"] == 8


def test_update_item_add_from_zero(app):
    app.dynamodb.create_table("counters", "id")
    app.dynamodb.put_item("counters", {"id": "new", "name": "test"})
    app.dynamodb.update_item("counters", {"id": "new"}, {"ADD": {"count": 1}})
    assert app.dynamodb.get_item("counters", {"id": "new"})["count"] == 1


def test_update_item_creates_if_missing(app):
    app.dynamodb.create_table("users", "id")
    item = app.dynamodb.update_item("users", {"id": "new-user"}, {"SET": {"name": "Bob"}})
    assert item["id"] == "new-user"
    assert item["name"] == "Bob"


def test_update_item_add_non_numeric_raises(app):
    app.dynamodb.create_table("t", "id")
    app.dynamodb.put_item("t", {"id": "1", "val": "string"})
    with pytest.raises(ValidationError, match="numeric"):
        app.dynamodb.update_item("t", {"id": "1"}, {"ADD": {"val": 1}})


def test_update_item_with_condition(app):
    app.dynamodb.create_table("t", "id")
    app.dynamodb.put_item("t", {"id": "1", "status": "active"})
    with pytest.raises(ValidationError, match="condition failed"):
        app.dynamodb.update_item(
            "t", {"id": "1"},
            {"SET": {"status": "deleted"}},
            condition={"function": "attribute_equals", "attribute": "status", "value": "inactive"},
        )


# ---------------------------------------------------------------------------
# TTL
# ---------------------------------------------------------------------------

def test_ttl_expired_item_not_returned(app):
    app.dynamodb.create_table("sessions", "id", ttl_attribute="expires_at")
    past = time.time() - 3600  # 1 hour ago
    app.dynamodb.put_item("sessions", {"id": "s1", "expires_at": past, "data": "old"})
    assert app.dynamodb.get_item("sessions", {"id": "s1"}) is None


def test_ttl_active_item_returned(app):
    app.dynamodb.create_table("sessions", "id", ttl_attribute="expires_at")
    future = time.time() + 3600  # 1 hour from now
    app.dynamodb.put_item("sessions", {"id": "s1", "expires_at": future, "data": "live"})
    item = app.dynamodb.get_item("sessions", {"id": "s1"})
    assert item is not None
    assert item["data"] == "live"


def test_ttl_expired_item_not_in_scan(app):
    app.dynamodb.create_table("sessions", "id", ttl_attribute="expires_at")
    past = time.time() - 1
    future = time.time() + 3600
    app.dynamodb.put_item("sessions", {"id": "s1", "expires_at": past})
    app.dynamodb.put_item("sessions", {"id": "s2", "expires_at": future})
    items = app.dynamodb.scan("sessions")
    assert len(items) == 1
    assert items[0]["id"] == "s2"


def test_update_ttl_configuration(app):
    app.dynamodb.create_table("t", "id")
    app.dynamodb.update_ttl("t", "expires_at")
    desc = app.dynamodb.describe_table("t")
    assert desc["ttl_attribute"] == "expires_at"
    app.dynamodb.update_ttl("t", None)
    desc2 = app.dynamodb.describe_table("t")
    assert desc2["ttl_attribute"] is None


# ---------------------------------------------------------------------------
# BatchGetItem
# ---------------------------------------------------------------------------

def test_batch_get_item(app):
    app.dynamodb.create_table("users", "id")
    app.dynamodb.create_table("orders", "order_id")
    app.dynamodb.put_item("users", {"id": "u1", "name": "Ada"})
    app.dynamodb.put_item("users", {"id": "u2", "name": "Bob"})
    app.dynamodb.put_item("orders", {"order_id": "o1", "total": 100})
    result = app.dynamodb.batch_get_item({
        "users": [{"id": "u1"}, {"id": "u2"}, {"id": "missing"}],
        "orders": [{"order_id": "o1"}],
    })
    users = result["users"]
    assert users[0]["name"] == "Ada"
    assert users[1]["name"] == "Bob"
    assert users[2] is None  # missing key
    assert result["orders"][0]["total"] == 100


def test_batch_get_item_empty_result(app):
    app.dynamodb.create_table("t", "id")
    result = app.dynamodb.batch_get_item({"t": [{"id": "nope"}]})
    assert result["t"] == [None]


# ---------------------------------------------------------------------------
# BatchWriteItem
# ---------------------------------------------------------------------------

def test_batch_write_put_and_delete(app):
    app.dynamodb.create_table("items", "id")
    app.dynamodb.put_item("items", {"id": "old", "v": 0})
    result = app.dynamodb.batch_write_item({
        "items": [
            {"put": {"id": "new1", "v": 1}},
            {"put": {"id": "new2", "v": 2}},
            {"delete": {"id": "old"}},
        ]
    })
    assert result["unprocessed"] == {}
    assert app.dynamodb.get_item("items", {"id": "new1"})["v"] == 1
    assert app.dynamodb.get_item("items", {"id": "new2"})["v"] == 2
    assert app.dynamodb.get_item("items", {"id": "old"}) is None


def test_batch_write_invalid_op_raises(app):
    app.dynamodb.create_table("t", "id")
    with pytest.raises(ValidationError):
        app.dynamodb.batch_write_item({"t": [{"update": {"id": "1"}}]})


# ---------------------------------------------------------------------------
# TransactWriteItems
# ---------------------------------------------------------------------------

def test_transact_write_all_or_nothing_success(app):
    app.dynamodb.create_table("accounts", "id")
    app.dynamodb.put_item("accounts", {"id": "A", "balance": 100})
    app.dynamodb.put_item("accounts", {"id": "B", "balance": 50})
    app.dynamodb.transact_write_items([
        {
            "update": {
                "table": "accounts",
                "key": {"id": "A"},
                "update_expression": {"ADD": {"balance": -30}},
            }
        },
        {
            "update": {
                "table": "accounts",
                "key": {"id": "B"},
                "update_expression": {"ADD": {"balance": 30}},
            }
        },
    ])
    assert app.dynamodb.get_item("accounts", {"id": "A"})["balance"] == 70
    assert app.dynamodb.get_item("accounts", {"id": "B"})["balance"] == 80


def test_transact_write_condition_failure_prevents_all(app):
    app.dynamodb.create_table("t", "id")
    app.dynamodb.put_item("t", {"id": "1", "v": 1})
    # condition fails because v != 99
    with pytest.raises(ValidationError, match="condition failed"):
        app.dynamodb.transact_write_items([
            {"put": {"table": "t", "item": {"id": "2", "v": 0}}},
            {
                "put": {
                    "table": "t",
                    "item": {"id": "1", "v": 2},
                    "condition": {
                        "function": "attribute_equals",
                        "attribute": "v",
                        "value": 99,
                    },
                }
            },
        ])
    # item 2 must NOT have been written (transaction rolled back)
    assert app.dynamodb.get_item("t", {"id": "2"}) is None
    # item 1 must remain unchanged
    assert app.dynamodb.get_item("t", {"id": "1"})["v"] == 1


def test_transact_write_invalid_op_raises(app):
    with pytest.raises(ValidationError):
        app.dynamodb.transact_write_items([{"noop": {}}])


# ---------------------------------------------------------------------------
# HTTP end-to-end for new DynamoDB operations
# ---------------------------------------------------------------------------

import json
import urllib.request
import urllib.error


def _jreq(url, payload):
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        url, data=data, method="POST",
        headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def test_dynamodb_http_update_item(server):
    url = server.base_url + "/dynamodb"
    _jreq(url, {"action": "create_table", "name": "ctr", "hash_key": "id"})
    _jreq(url, {"action": "put_item", "table": "ctr", "item": {"id": "x", "n": 5}})
    status, resp = _jreq(url, {
        "action": "update_item",
        "table": "ctr",
        "key": {"id": "x"},
        "update_expression": {"ADD": {"n": 3}},
    })
    assert status == 200
    assert resp["item"]["n"] == 8


def test_dynamodb_http_batch_get(server):
    url = server.base_url + "/dynamodb"
    _jreq(url, {"action": "create_table", "name": "bg", "hash_key": "id"})
    _jreq(url, {"action": "put_item", "table": "bg", "item": {"id": "1", "v": "a"}})
    _jreq(url, {"action": "put_item", "table": "bg", "item": {"id": "2", "v": "b"}})
    status, resp = _jreq(url, {
        "action": "batch_get_item",
        "request_items": {"bg": [{"id": "1"}, {"id": "2"}]},
    })
    assert status == 200
    items = resp["responses"]["bg"]
    assert {i["v"] for i in items if i} == {"a", "b"}


def test_dynamodb_http_batch_write(server):
    url = server.base_url + "/dynamodb"
    _jreq(url, {"action": "create_table", "name": "bw", "hash_key": "id"})
    status, resp = _jreq(url, {
        "action": "batch_write_item",
        "request_items": {
            "bw": [
                {"put": {"id": "1", "v": 1}},
                {"put": {"id": "2", "v": 2}},
            ]
        },
    })
    assert status == 200
    assert resp["unprocessed"] == {}
    _, scan = _jreq(url, {"action": "scan", "table": "bw"})
    assert len(scan["items"]) == 2


def test_dynamodb_http_transact_write(server):
    url = server.base_url + "/dynamodb"
    _jreq(url, {"action": "create_table", "name": "tw", "hash_key": "id"})
    _jreq(url, {"action": "put_item", "table": "tw", "item": {"id": "a", "v": 10}})
    status, resp = _jreq(url, {
        "action": "transact_write_items",
        "transact_items": [
            {"put": {"table": "tw", "item": {"id": "b", "v": 20}}},
            {"update": {
                "table": "tw",
                "key": {"id": "a"},
                "update_expression": {"ADD": {"v": 5}},
            }},
        ],
    })
    assert status == 200
    assert resp["transacted"] == 2
    _, ga = _jreq(url, {"action": "get_item", "table": "tw", "key": {"id": "a"}})
    assert ga["item"]["v"] == 15


def test_dynamodb_http_conditional_put_fail(server):
    url = server.base_url + "/dynamodb"
    _jreq(url, {"action": "create_table", "name": "cond", "hash_key": "id"})
    _jreq(url, {"action": "put_item", "table": "cond", "item": {"id": "1"}})
    status, resp = _jreq(url, {
        "action": "put_item",
        "table": "cond",
        "item": {"id": "1"},
        "condition": {"function": "attribute_not_exists", "attribute": "id"},
    })
    assert status == 400
    assert "condition failed" in resp["message"]
