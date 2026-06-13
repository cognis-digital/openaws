import pytest

from openaws.errors import Conflict, NotFound, ValidationError


def test_create_and_describe_table(app):
    app.dynamodb.create_table("users", "id")
    desc = app.dynamodb.describe_table("users")
    assert desc["hash_key"] == "id"
    assert desc["range_key"] is None
    assert app.dynamodb.list_tables() == ["users"]


def test_duplicate_table_conflicts(app):
    app.dynamodb.create_table("t", "id")
    with pytest.raises(Conflict):
        app.dynamodb.create_table("t", "id")


def test_put_get_item_hash_only(app):
    app.dynamodb.create_table("users", "id")
    app.dynamodb.put_item("users", {"id": "u1", "name": "Ada", "age": 36})
    got = app.dynamodb.get_item("users", {"id": "u1"})
    assert got["name"] == "Ada"
    assert got["age"] == 36


def test_get_missing_returns_none(app):
    app.dynamodb.create_table("users", "id")
    assert app.dynamodb.get_item("users", {"id": "ghost"}) is None


def test_put_requires_partition_key(app):
    app.dynamodb.create_table("users", "id")
    with pytest.raises(ValidationError):
        app.dynamodb.put_item("users", {"name": "no-id"})


def test_composite_key_and_query(app):
    app.dynamodb.create_table("events", "user", "ts")
    app.dynamodb.put_item("events", {"user": "u1", "ts": "2026-01-01", "v": 1})
    app.dynamodb.put_item("events", {"user": "u1", "ts": "2026-02-01", "v": 2})
    app.dynamodb.put_item("events", {"user": "u2", "ts": "2026-01-01", "v": 9})
    rows = app.dynamodb.query("events", "u1")
    assert [r["v"] for r in rows] == [1, 2]


def test_query_sort_conditions(app):
    app.dynamodb.create_table("events", "user", "ts")
    for ts in ["2026-01", "2026-02", "2027-01"]:
        app.dynamodb.put_item("events", {"user": "u1", "ts": ts})
    begins = app.dynamodb.query("events", "u1", sort_begins_with="2026")
    assert {r["ts"] for r in begins} == {"2026-01", "2026-02"}
    eq = app.dynamodb.query("events", "u1", sort_eq="2027-01")
    assert len(eq) == 1 and eq[0]["ts"] == "2027-01"


def test_scan_and_filter(app):
    app.dynamodb.create_table("users", "id")
    app.dynamodb.put_item("users", {"id": "1", "team": "a"})
    app.dynamodb.put_item("users", {"id": "2", "team": "b"})
    app.dynamodb.put_item("users", {"id": "3", "team": "a"})
    assert len(app.dynamodb.scan("users")) == 3
    a_team = app.dynamodb.scan("users", {"team": "a"})
    assert {u["id"] for u in a_team} == {"1", "3"}


def test_delete_item_and_table(app):
    app.dynamodb.create_table("users", "id")
    app.dynamodb.put_item("users", {"id": "1"})
    app.dynamodb.delete_item("users", {"id": "1"})
    assert app.dynamodb.get_item("users", {"id": "1"}) is None
    app.dynamodb.delete_table("users")
    with pytest.raises(NotFound):
        app.dynamodb.describe_table("users")


def test_put_overwrites_item(app):
    app.dynamodb.create_table("users", "id")
    app.dynamodb.put_item("users", {"id": "1", "v": 1})
    app.dynamodb.put_item("users", {"id": "1", "v": 2})
    assert app.dynamodb.get_item("users", {"id": "1"})["v"] == 2
    assert len(app.dynamodb.scan("users")) == 1


def test_missing_table_raises(app):
    with pytest.raises(NotFound):
        app.dynamodb.put_item("nope", {"id": "1"})
