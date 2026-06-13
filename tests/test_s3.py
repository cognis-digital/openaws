import pytest

from openaws.errors import Conflict, NotFound, ValidationError


def test_create_list_delete_bucket(app):
    app.s3.create_bucket("my-bucket")
    assert [b["name"] for b in app.s3.list_buckets()] == ["my-bucket"]
    app.s3.delete_bucket("my-bucket")
    assert app.s3.list_buckets() == []


def test_bucket_name_validation(app):
    with pytest.raises(ValidationError):
        app.s3.create_bucket("UPPER")
    with pytest.raises(ValidationError):
        app.s3.create_bucket("x")


def test_duplicate_bucket_conflicts(app):
    app.s3.create_bucket("dup")
    with pytest.raises(Conflict):
        app.s3.create_bucket("dup")


def test_object_round_trip(app):
    app.s3.create_bucket("data")
    res = app.s3.put_object("data", "hello.txt", b"hello world", "text/plain")
    assert res["size"] == 11
    got = app.s3.get_object("data", "hello.txt")
    assert got["body"] == b"hello world"
    assert got["content_type"] == "text/plain"
    assert got["etag"] == res["etag"]


def test_put_overwrites_and_updates_etag(app):
    app.s3.create_bucket("data")
    a = app.s3.put_object("data", "k", b"one")
    b = app.s3.put_object("data", "k", b"two-longer")
    assert a["etag"] != b["etag"]
    assert app.s3.get_object("data", "k")["body"] == b"two-longer"


def test_list_objects_with_prefix(app):
    app.s3.create_bucket("data")
    app.s3.put_object("data", "logs/a", b"1")
    app.s3.put_object("data", "logs/b", b"2")
    app.s3.put_object("data", "img/c", b"3")
    result = app.s3.list_objects("data", "logs/")
    keys = [o["key"] for o in result["objects"]]
    assert keys == ["logs/a", "logs/b"]


def test_delete_object(app):
    app.s3.create_bucket("data")
    app.s3.put_object("data", "k", b"v")
    app.s3.delete_object("data", "k")
    with pytest.raises(NotFound):
        app.s3.get_object("data", "k")


def test_missing_bucket_and_key(app):
    with pytest.raises(NotFound):
        app.s3.get_object("nope", "k")
    app.s3.create_bucket("data")
    with pytest.raises(NotFound):
        app.s3.get_object("data", "missing")


def test_delete_nonempty_bucket_conflicts(app):
    app.s3.create_bucket("data")
    app.s3.put_object("data", "k", b"v")
    with pytest.raises(Conflict):
        app.s3.delete_bucket("data")


def test_string_body_is_encoded(app):
    app.s3.create_bucket("data")
    app.s3.put_object("data", "k", "abc")
    assert app.s3.get_object("data", "k")["body"] == b"abc"
