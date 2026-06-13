"""Tests for the extended S3 features:
multipart upload, versioning, tagging, copy, prefix/delimiter listing,
per-object metadata, and presigned-URL token stub.
"""

import pytest

from openaws.errors import Conflict, NotFound, ValidationError


# ---------------------------------------------------------------------------
# list_objects delimiter / common_prefixes
# ---------------------------------------------------------------------------

def test_list_objects_returns_objects_and_common_prefixes(app):
    app.s3.create_bucket("data")
    for key in ["a/1", "a/2", "b/1", "top"]:
        app.s3.put_object("data", key, b"x")
    result = app.s3.list_objects("data", prefix="", delimiter="/")
    assert {o["key"] for o in result["objects"]} == {"top"}
    assert set(result["common_prefixes"]) == {"a/", "b/"}


def test_list_objects_no_delimiter_returns_all(app):
    app.s3.create_bucket("data")
    app.s3.put_object("data", "x/y/z", b"1")
    app.s3.put_object("data", "x/y/w", b"2")
    result = app.s3.list_objects("data", prefix="x/")
    assert len(result["objects"]) == 2
    assert result["common_prefixes"] == []


def test_list_objects_max_keys(app):
    app.s3.create_bucket("data")
    for i in range(5):
        app.s3.put_object("data", f"k{i}", b"v")
    result = app.s3.list_objects("data", max_keys=3)
    assert len(result["objects"]) == 3


# ---------------------------------------------------------------------------
# Object metadata
# ---------------------------------------------------------------------------

def test_put_and_get_object_metadata(app):
    app.s3.create_bucket("data")
    meta = {"author": "Ada", "project": "openaws"}
    app.s3.put_object("data", "readme.md", b"# hi", metadata=meta)
    obj = app.s3.get_object("data", "readme.md")
    assert obj["metadata"]["author"] == "Ada"
    assert obj["metadata"]["project"] == "openaws"


def test_object_without_metadata_returns_empty_dict(app):
    app.s3.create_bucket("data")
    app.s3.put_object("data", "k", b"v")
    obj = app.s3.get_object("data", "k")
    assert obj["metadata"] == {}


# ---------------------------------------------------------------------------
# Versioning
# ---------------------------------------------------------------------------

def test_versioning_default_disabled(app):
    app.s3.create_bucket("v-test")
    info = app.s3.get_bucket_versioning("v-test")
    assert info["versioning"] == "disabled"


def test_enable_versioning(app):
    app.s3.create_bucket("v-test")
    app.s3.put_bucket_versioning("v-test", "enabled")
    assert app.s3.get_bucket_versioning("v-test")["versioning"] == "enabled"


def test_versioning_invalid_status(app):
    app.s3.create_bucket("v-test")
    with pytest.raises(ValidationError):
        app.s3.put_bucket_versioning("v-test", "bad-status")


def test_versioned_put_creates_version_ids(app):
    app.s3.create_bucket("v-test")
    app.s3.put_bucket_versioning("v-test", "enabled")
    r1 = app.s3.put_object("v-test", "doc.txt", b"v1")
    r2 = app.s3.put_object("v-test", "doc.txt", b"v2")
    assert "version_id" in r1
    assert "version_id" in r2
    assert r1["version_id"] != r2["version_id"]


def test_get_specific_version(app):
    app.s3.create_bucket("v-test")
    app.s3.put_bucket_versioning("v-test", "enabled")
    r1 = app.s3.put_object("v-test", "f.txt", b"first")
    r2 = app.s3.put_object("v-test", "f.txt", b"second")
    got_v1 = app.s3.get_object("v-test", "f.txt", version_id=r1["version_id"])
    got_v2 = app.s3.get_object("v-test", "f.txt", version_id=r2["version_id"])
    assert got_v1["body"] == b"first"
    assert got_v2["body"] == b"second"
    # latest via no version_id
    latest = app.s3.get_object("v-test", "f.txt")
    assert latest["body"] == b"second"


def test_list_object_versions(app):
    app.s3.create_bucket("v-test")
    app.s3.put_bucket_versioning("v-test", "enabled")
    app.s3.put_object("v-test", "f.txt", b"v1")
    app.s3.put_object("v-test", "f.txt", b"v2")
    info = app.s3.list_object_versions("v-test")
    versions = info["versions"]
    assert len(versions) == 2
    is_latest_list = [v["is_latest"] for v in versions]
    assert sum(is_latest_list) == 1  # exactly one latest


def test_delete_specific_version_promotes_next(app):
    app.s3.create_bucket("v-test")
    app.s3.put_bucket_versioning("v-test", "enabled")
    r1 = app.s3.put_object("v-test", "f.txt", b"v1")
    r2 = app.s3.put_object("v-test", "f.txt", b"v2")
    # delete the latest version; v1 should be promoted
    app.s3.delete_object("v-test", "f.txt", version_id=r2["version_id"])
    remaining = app.s3.list_object_versions("v-test")["versions"]
    assert len(remaining) == 1
    assert remaining[0]["version_id"] == r1["version_id"]


# ---------------------------------------------------------------------------
# Tagging
# ---------------------------------------------------------------------------

def test_put_get_delete_tags(app):
    app.s3.create_bucket("data")
    app.s3.put_object("data", "k", b"v")
    app.s3.put_object_tagging("data", "k", {"env": "prod", "owner": "ada"})
    tags = app.s3.get_object_tagging("data", "k")
    assert tags == {"env": "prod", "owner": "ada"}
    app.s3.delete_object_tagging("data", "k")
    assert app.s3.get_object_tagging("data", "k") == {}


def test_put_tags_replaces_old_tags(app):
    app.s3.create_bucket("data")
    app.s3.put_object("data", "k", b"v")
    app.s3.put_object_tagging("data", "k", {"old": "1"})
    app.s3.put_object_tagging("data", "k", {"new": "2"})
    tags = app.s3.get_object_tagging("data", "k")
    assert "old" not in tags
    assert tags["new"] == "2"


def test_tag_missing_object_raises(app):
    app.s3.create_bucket("data")
    with pytest.raises(NotFound):
        app.s3.put_object_tagging("data", "nope", {"k": "v"})


# ---------------------------------------------------------------------------
# Copy
# ---------------------------------------------------------------------------

def test_copy_object(app):
    app.s3.create_bucket("src")
    app.s3.create_bucket("dst")
    app.s3.put_object("src", "original.txt", b"hello copy", "text/plain")
    result = app.s3.copy_object("src", "original.txt", "dst", "copy.txt")
    assert result["key"] == "copy.txt"
    got = app.s3.get_object("dst", "copy.txt")
    assert got["body"] == b"hello copy"
    assert got["content_type"] == "text/plain"


def test_copy_object_with_new_metadata(app):
    app.s3.create_bucket("src")
    app.s3.create_bucket("dst")
    app.s3.put_object("src", "f.txt", b"data", metadata={"original": "yes"})
    app.s3.copy_object("src", "f.txt", "dst", "f2.txt", metadata={"copied": "true"})
    obj = app.s3.get_object("dst", "f2.txt")
    assert obj["metadata"] == {"copied": "true"}


def test_copy_missing_source_raises(app):
    app.s3.create_bucket("src")
    app.s3.create_bucket("dst")
    with pytest.raises(NotFound):
        app.s3.copy_object("src", "nope.txt", "dst", "copy.txt")


# ---------------------------------------------------------------------------
# Multipart upload
# ---------------------------------------------------------------------------

def test_multipart_upload_round_trip(app):
    app.s3.create_bucket("data")
    up = app.s3.create_multipart_upload("data", "big.bin", "application/octet-stream")
    upload_id = up["upload_id"]
    # upload 3 parts
    part1 = b"A" * 5_242_880  # 5 MiB
    part2 = b"B" * 5_242_880
    part3 = b"C" * 1_000
    p1 = app.s3.upload_part("data", "big.bin", upload_id, 1, part1)
    p2 = app.s3.upload_part("data", "big.bin", upload_id, 2, part2)
    p3 = app.s3.upload_part("data", "big.bin", upload_id, 3, part3)
    parts = [
        {"part_number": 1, "etag": p1["etag"]},
        {"part_number": 2, "etag": p2["etag"]},
        {"part_number": 3, "etag": p3["etag"]},
    ]
    result = app.s3.complete_multipart_upload("data", "big.bin", upload_id, parts)
    assert result["key"] == "big.bin"
    assert "-3" in result["etag"]  # multipart etag suffix
    obj = app.s3.get_object("data", "big.bin")
    assert obj["body"] == part1 + part2 + part3


def test_list_parts(app):
    app.s3.create_bucket("data")
    up = app.s3.create_multipart_upload("data", "f", "application/octet-stream")
    uid = up["upload_id"]
    app.s3.upload_part("data", "f", uid, 1, b"hello")
    app.s3.upload_part("data", "f", uid, 2, b"world")
    parts = app.s3.list_parts("data", "f", uid)
    assert [p["part_number"] for p in parts] == [1, 2]


def test_abort_multipart_upload(app):
    app.s3.create_bucket("data")
    up = app.s3.create_multipart_upload("data", "temp", "application/octet-stream")
    uid = up["upload_id"]
    app.s3.upload_part("data", "temp", uid, 1, b"data")
    app.s3.abort_multipart_upload("data", "temp", uid)
    with pytest.raises(NotFound):
        app.s3.list_parts("data", "temp", uid)


def test_upload_part_invalid_part_number(app):
    app.s3.create_bucket("data")
    up = app.s3.create_multipart_upload("data", "f", "application/octet-stream")
    with pytest.raises(ValidationError):
        app.s3.upload_part("data", "f", up["upload_id"], 0, b"data")


def test_multipart_missing_upload_id(app):
    app.s3.create_bucket("data")
    with pytest.raises(NotFound):
        app.s3.upload_part("data", "f", "bad-upload-id", 1, b"data")


# ---------------------------------------------------------------------------
# Presigned URL token stub
# ---------------------------------------------------------------------------

def test_generate_and_verify_presigned_url(app):
    app.s3.create_bucket("data")
    info = app.s3.generate_presigned_url("data", "report.pdf", expires_in=300)
    assert "url" in info
    assert "token" in info
    assert info["expires_at"] > 0
    verified = app.s3.verify_presigned_token(info["token"])
    assert verified["valid"] is True
    assert verified["bucket"] == "data"
    assert verified["key"] == "report.pdf"
    assert verified["operation"] == "get_object"


def test_presigned_url_tampered_token_rejected(app):
    app.s3.create_bucket("data")
    info = app.s3.generate_presigned_url("data", "f", expires_in=60)
    bad_token = info["token"] + "TAMPERED"
    with pytest.raises(ValidationError):
        app.s3.verify_presigned_token(bad_token)


def test_presigned_url_expired_raises(app):
    app.s3.create_bucket("data")
    # expires_in=-1 means the expiry timestamp is already in the past
    info = app.s3.generate_presigned_url("data", "f", expires_in=-1)
    with pytest.raises(ValidationError, match="expired"):
        app.s3.verify_presigned_token(info["token"])


# ---------------------------------------------------------------------------
# HTTP end-to-end for new S3 operations
# ---------------------------------------------------------------------------

import json
import urllib.error
import urllib.request


def _req(url, method="GET", data=None, headers=None):
    req = urllib.request.Request(url, data=data, method=method, headers=headers or {})
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, resp.read(), dict(resp.headers)
    except urllib.error.HTTPError as e:
        return e.code, e.read(), dict(e.headers)


def _json_req(url, method="GET", payload=None):
    data = json.dumps(payload).encode() if payload is not None else None
    hdrs = {"Content-Type": "application/json"} if data else {}
    status, body, _ = _req(url, method, data, hdrs)
    return status, json.loads(body) if body else None


def test_s3_http_versioning_round_trip(server):
    base = server.base_url
    # create bucket (unique name per server instance)
    bkt = "vbkt-v2"
    _json_req(base + f"/s3/{bkt}", method="PUT")
    # enable versioning
    status, body = _json_req(
        base + f"/s3/{bkt}?versioning", method="PUT",
        payload={"status": "enabled"}
    )
    assert status == 200
    # put two versions
    _req(base + f"/s3/{bkt}/doc.txt", "PUT", b"v1", {"Content-Type": "text/plain"})
    _req(base + f"/s3/{bkt}/doc.txt", "PUT", b"v2", {"Content-Type": "text/plain"})
    # list versions
    status, info = _json_req(base + f"/s3/{bkt}?versions")
    assert status == 200
    assert len(info["versions"]) == 2


def test_s3_http_tagging(server):
    base = server.base_url
    _json_req(base + "/s3/tbkt-v2", method="PUT")
    _req(base + "/s3/tbkt-v2/obj", "PUT", b"data", {"Content-Type": "text/plain"})
    # put tags — PUT to tagging sub-resource returns 200 (tagged: key)
    status, tag_put = _json_req(
        base + "/s3/tbkt-v2/obj?tagging", method="PUT",
        payload={"env": "test"}
    )
    assert status == 200
    assert tag_put["tagged"] == "obj"
    # get tags
    status, tags_resp = _json_req(base + "/s3/tbkt-v2/obj?tagging")
    assert status == 200
    assert tags_resp["tags"]["env"] == "test"
    # delete tags
    status, _ = _json_req(base + "/s3/tbkt-v2/obj?tagging", method="DELETE")
    assert status == 200
    _, empty = _json_req(base + "/s3/tbkt-v2/obj?tagging")
    assert empty["tags"] == {}


def test_s3_http_copy(server):
    base = server.base_url
    _json_req(base + "/s3/src", method="PUT")
    _json_req(base + "/s3/dst", method="PUT")
    _req(base + "/s3/src/f.txt", "PUT", b"copy me", {"Content-Type": "text/plain"})
    status, result = _json_req(
        base + "/s3/dst/f-copy.txt", method="PUT",
        payload=None,
    )
    # use x-amz-copy-source header
    req = urllib.request.Request(
        base + "/s3/dst/f-copy.txt",
        method="PUT",
        headers={"x-amz-copy-source": "/src/f.txt", "Content-Length": "0"},
    )
    with urllib.request.urlopen(req) as resp:
        assert resp.status == 200
    status, body, _ = _req(base + "/s3/dst/f-copy.txt", "GET")
    assert body == b"copy me"


def test_s3_http_multipart(server):
    base = server.base_url
    _json_req(base + "/s3/mp-bkt-v2", method="PUT")
    # initiate
    status, up = _json_req(base + "/s3/mp-bkt-v2/large.bin?uploads", method="PUT")
    assert status == 200
    uid = up["upload_id"]
    # upload 2 parts — each returns 200 (part metadata)
    p1_status, p1_body, _ = _req(
        base + f"/s3/mp-bkt-v2/large.bin?uploadId={uid}&partNumber=1",
        "PUT", b"A" * 100, {"Content-Type": "application/octet-stream"}
    )
    p2_status, p2_body, _ = _req(
        base + f"/s3/mp-bkt-v2/large.bin?uploadId={uid}&partNumber=2",
        "PUT", b"B" * 50, {"Content-Type": "application/octet-stream"}
    )
    assert p1_status == 200
    assert p2_status == 200
    p1 = json.loads(p1_body)
    p2 = json.loads(p2_body)
    # list parts
    status, parts_resp = _json_req(base + f"/s3/mp-bkt-v2/large.bin?uploadId={uid}")
    assert status == 200
    assert len(parts_resp["parts"]) == 2
    # complete
    status, result = _json_req(
        base + f"/s3/mp-bkt-v2/large.bin?uploadId={uid}",
        method="POST",
        payload={"parts": [
            {"part_number": 1, "etag": p1["etag"]},
            {"part_number": 2, "etag": p2["etag"]},
        ]},
    )
    assert status == 200
    assert "-2" in result["etag"]
    # get final object
    status, body, _ = _req(base + "/s3/mp-bkt-v2/large.bin", "GET")
    assert status == 200
    assert body == b"A" * 100 + b"B" * 50


def test_s3_http_delimiter_listing(server):
    base = server.base_url
    _json_req(base + "/s3/delim-bkt", method="PUT")
    for key in ["a/1", "a/2", "b/1", "top"]:
        _req(base + f"/s3/delim-bkt/{key}", "PUT", b"x", {"Content-Type": "text/plain"})
    status, resp = _json_req(base + "/s3/delim-bkt?delimiter=/")
    assert status == 200
    assert set(resp["common_prefixes"]) == {"a/", "b/"}
    assert any(o["key"] == "top" for o in resp["objects"])
