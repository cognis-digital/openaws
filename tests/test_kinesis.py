"""Tests for Kinesis Data Streams:
streams/shards, PutRecord, PutRecords, GetShardIterator, GetRecords.
"""

import base64
import json
import time
import urllib.error
import urllib.request

import pytest

from openaws.errors import Conflict, NotFound, ValidationError


# ---------------------------------------------------------------------------
# Stream management
# ---------------------------------------------------------------------------

def test_create_list_delete_stream(app):
    app.kinesis.create_stream("my-stream")
    assert "my-stream" in app.kinesis.list_streams()
    app.kinesis.delete_stream("my-stream")
    assert "my-stream" not in app.kinesis.list_streams()


def test_create_duplicate_stream_conflicts(app):
    app.kinesis.create_stream("dup")
    with pytest.raises(Conflict):
        app.kinesis.create_stream("dup")


def test_create_stream_invalid_shard_count(app):
    with pytest.raises(ValidationError):
        app.kinesis.create_stream("bad", shard_count=0)


def test_create_stream_requires_name(app):
    with pytest.raises(ValidationError):
        app.kinesis.create_stream("")


def test_describe_stream(app):
    app.kinesis.create_stream("s1", shard_count=2)
    desc = app.kinesis.describe_stream("s1")
    assert desc["name"] == "s1"
    assert desc["shard_count"] == 2
    assert len(desc["shards"]) == 2
    assert desc["status"] == "ACTIVE"


def test_describe_missing_stream_raises(app):
    with pytest.raises(NotFound):
        app.kinesis.describe_stream("nope")


# ---------------------------------------------------------------------------
# PutRecord
# ---------------------------------------------------------------------------

def test_put_record_bytes(app):
    app.kinesis.create_stream("s")
    result = app.kinesis.put_record("s", b"hello", "pk1")
    assert "sequence_number" in result
    assert result["sequence_number"] == "00000000000000000001"
    assert result["shard_id"].startswith("shardId-")


def test_put_record_string_treated_as_b64(app):
    app.kinesis.create_stream("s")
    payload = base64.b64encode(b"test data").decode()
    result = app.kinesis.put_record("s", payload, "pk")
    assert "sequence_number" in result


def test_put_record_plain_string_auto_encoded(app):
    app.kinesis.create_stream("s")
    result = app.kinesis.put_record("s", "not base64 !!!", "pk")
    assert "sequence_number" in result


def test_put_record_sequence_numbers_increase(app):
    app.kinesis.create_stream("s")
    r1 = app.kinesis.put_record("s", b"a", "pk")
    r2 = app.kinesis.put_record("s", b"b", "pk")
    assert int(r2["sequence_number"]) > int(r1["sequence_number"])


def test_put_record_missing_partition_key_raises(app):
    app.kinesis.create_stream("s")
    with pytest.raises(ValidationError):
        app.kinesis.put_record("s", b"data", "")


def test_put_record_missing_stream_raises(app):
    with pytest.raises(NotFound):
        app.kinesis.put_record("nope", b"data", "pk")


def test_put_record_shard_assignment(app):
    app.kinesis.create_stream("s", shard_count=4)
    # same partition key must always go to the same shard
    r1 = app.kinesis.put_record("s", b"a", "same-key")
    r2 = app.kinesis.put_record("s", b"b", "same-key")
    assert r1["shard_id"] == r2["shard_id"]


# ---------------------------------------------------------------------------
# PutRecords
# ---------------------------------------------------------------------------

def test_put_records_batch(app):
    app.kinesis.create_stream("s")
    records = [
        {"data": b"a", "partition_key": "k1"},
        {"data": b"b", "partition_key": "k2"},
        {"data": b"c", "partition_key": "k3"},
    ]
    result = app.kinesis.put_records("s", records)
    assert result["failed_record_count"] == 0
    assert len(result["records"]) == 3
    for r in result["records"]:
        assert "sequence_number" in r


def test_put_records_empty_raises(app):
    app.kinesis.create_stream("s")
    with pytest.raises(ValidationError):
        app.kinesis.put_records("s", [])


# ---------------------------------------------------------------------------
# GetShardIterator + GetRecords
# ---------------------------------------------------------------------------

def test_get_records_trim_horizon(app):
    app.kinesis.create_stream("s")
    app.kinesis.put_record("s", b"msg1", "pk")
    app.kinesis.put_record("s", b"msg2", "pk")
    it = app.kinesis.get_shard_iterator("s", "shardId-000000000000", "TRIM_HORIZON")
    result = app.kinesis.get_records(it["shard_iterator"])
    assert len(result["records"]) == 2
    # decode base64 and check payload
    bodies = [base64.b64decode(r["data"]) for r in result["records"]]
    assert bodies[0] == b"msg1"
    assert bodies[1] == b"msg2"


def test_get_records_latest_sees_new_records(app):
    app.kinesis.create_stream("s")
    app.kinesis.put_record("s", b"old", "pk")
    it = app.kinesis.get_shard_iterator("s", "shardId-000000000000", "LATEST")
    # no records yet at LATEST
    r1 = app.kinesis.get_records(it["shard_iterator"])
    assert r1["records"] == []
    # add a new record
    app.kinesis.put_record("s", b"new", "pk")
    # consume with the next iterator
    r2 = app.kinesis.get_records(r1["next_shard_iterator"])
    assert len(r2["records"]) == 1
    assert base64.b64decode(r2["records"][0]["data"]) == b"new"


def test_get_records_at_sequence_number(app):
    app.kinesis.create_stream("s")
    r1 = app.kinesis.put_record("s", b"first", "pk")
    app.kinesis.put_record("s", b"second", "pk")
    seq1 = r1["sequence_number"]
    it = app.kinesis.get_shard_iterator(
        "s", "shardId-000000000000", "AT_SEQUENCE_NUMBER", seq1
    )
    result = app.kinesis.get_records(it["shard_iterator"])
    assert base64.b64decode(result["records"][0]["data"]) == b"first"
    assert len(result["records"]) == 2


def test_get_records_after_sequence_number(app):
    app.kinesis.create_stream("s")
    r1 = app.kinesis.put_record("s", b"first", "pk")
    app.kinesis.put_record("s", b"second", "pk")
    seq1 = r1["sequence_number"]
    it = app.kinesis.get_shard_iterator(
        "s", "shardId-000000000000", "AFTER_SEQUENCE_NUMBER", seq1
    )
    result = app.kinesis.get_records(it["shard_iterator"])
    assert len(result["records"]) == 1
    assert base64.b64decode(result["records"][0]["data"]) == b"second"


def test_get_records_uses_next_iterator(app):
    app.kinesis.create_stream("s")
    for i in range(5):
        app.kinesis.put_record("s", f"msg{i}".encode(), "pk")
    it = app.kinesis.get_shard_iterator("s", "shardId-000000000000", "TRIM_HORIZON")
    r1 = app.kinesis.get_records(it["shard_iterator"], limit=2)
    assert len(r1["records"]) == 2
    r2 = app.kinesis.get_records(r1["next_shard_iterator"], limit=3)
    assert len(r2["records"]) == 3


def test_get_records_invalid_iterator_raises(app):
    app.kinesis.create_stream("s")
    with pytest.raises(ValidationError, match="invalid or expired"):
        app.kinesis.get_records("not-a-real-iterator")


def test_iterator_consumed_after_get_records(app):
    app.kinesis.create_stream("s")
    it = app.kinesis.get_shard_iterator("s", "shardId-000000000000", "TRIM_HORIZON")
    it_id = it["shard_iterator"]
    app.kinesis.get_records(it_id)
    # old iterator must be consumed and no longer valid
    with pytest.raises(ValidationError):
        app.kinesis.get_records(it_id)


def test_get_shard_iterator_invalid_type(app):
    app.kinesis.create_stream("s")
    with pytest.raises(ValidationError, match="invalid iterator_type"):
        app.kinesis.get_shard_iterator("s", "shardId-000000000000", "BOGUS")


def test_get_shard_iterator_out_of_range_shard(app):
    app.kinesis.create_stream("s", shard_count=1)
    with pytest.raises(ValidationError, match="out of range"):
        app.kinesis.get_shard_iterator("s", "shardId-000000000005", "TRIM_HORIZON")


def test_sequence_number_in_record_matches_put(app):
    app.kinesis.create_stream("s")
    put_result = app.kinesis.put_record("s", b"data", "pk")
    it = app.kinesis.get_shard_iterator("s", "shardId-000000000000", "TRIM_HORIZON")
    records = app.kinesis.get_records(it["shard_iterator"])["records"]
    assert records[0]["sequence_number"] == put_result["sequence_number"]


def test_describe_stream_shows_last_sequence_after_puts(app):
    app.kinesis.create_stream("s")
    app.kinesis.put_record("s", b"a", "pk")
    app.kinesis.put_record("s", b"b", "pk")
    desc = app.kinesis.describe_stream("s")
    shard0 = desc["shards"][0]
    assert "last_sequence_number" in shard0
    assert int(shard0["last_sequence_number"]) == 2


def test_delete_stream_clears_records(app):
    app.kinesis.create_stream("s")
    app.kinesis.put_record("s", b"data", "pk")
    app.kinesis.delete_stream("s")
    app.kinesis.create_stream("s")
    it = app.kinesis.get_shard_iterator("s", "shardId-000000000000", "TRIM_HORIZON")
    result = app.kinesis.get_records(it["shard_iterator"])
    assert result["records"] == []


def test_multi_shard_puts(app):
    app.kinesis.create_stream("ms", shard_count=3)
    for i in range(30):
        app.kinesis.put_record("ms", f"msg{i}".encode(), f"key-{i}")
    # all records must be retrievable across all shards
    total = 0
    for shard_idx in range(3):
        it = app.kinesis.get_shard_iterator(
            "ms", f"shardId-{shard_idx:012d}", "TRIM_HORIZON"
        )
        result = app.kinesis.get_records(it["shard_iterator"], limit=30)
        total += len(result["records"])
    assert total == 30


# ---------------------------------------------------------------------------
# HTTP end-to-end for Kinesis
# ---------------------------------------------------------------------------

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


def test_kinesis_http_round_trip(server):
    url = server.base_url + "/kinesis"

    # create stream
    status, resp = _jreq(url, {"action": "create_stream", "name": "test-stream"})
    assert status == 200
    assert resp["name"] == "test-stream"

    # list streams
    status, resp = _jreq(url, {"action": "list_streams"})
    assert "test-stream" in resp["streams"]

    # put record
    data_b64 = base64.b64encode(b"hello kinesis").decode()
    status, put_resp = _jreq(url, {
        "action": "put_record",
        "stream": "test-stream",
        "data": data_b64,
        "partition_key": "pk1",
    })
    assert status == 200
    assert "sequence_number" in put_resp

    # get shard iterator
    status, it_resp = _jreq(url, {
        "action": "get_shard_iterator",
        "stream": "test-stream",
        "shard_id": "shardId-000000000000",
        "iterator_type": "TRIM_HORIZON",
    })
    assert status == 200
    shard_it = it_resp["shard_iterator"]

    # get records
    status, records_resp = _jreq(url, {
        "action": "get_records",
        "shard_iterator": shard_it,
    })
    assert status == 200
    assert len(records_resp["records"]) == 1
    assert base64.b64decode(records_resp["records"][0]["data"]) == b"hello kinesis"


def test_kinesis_http_put_records_batch(server):
    url = server.base_url + "/kinesis"
    _jreq(url, {"action": "create_stream", "name": "batch-stream"})
    records = [
        {"data": base64.b64encode(f"msg{i}".encode()).decode(), "partition_key": f"pk{i}"}
        for i in range(5)
    ]
    status, resp = _jreq(url, {
        "action": "put_records",
        "stream": "batch-stream",
        "records": records,
    })
    assert status == 200
    assert resp["failed_record_count"] == 0
    assert len(resp["records"]) == 5


def test_kinesis_http_describe_stream(server):
    url = server.base_url + "/kinesis"
    _jreq(url, {"action": "create_stream", "name": "desc-stream", "shard_count": 2})
    status, resp = _jreq(url, {"action": "describe_stream", "name": "desc-stream"})
    assert status == 200
    assert resp["shard_count"] == 2
    assert len(resp["shards"]) == 2


def test_kinesis_http_delete_stream(server):
    url = server.base_url + "/kinesis"
    _jreq(url, {"action": "create_stream", "name": "del-stream"})
    status, resp = _jreq(url, {"action": "delete_stream", "name": "del-stream"})
    assert status == 200
    assert resp["deleted"] == "del-stream"
    _, streams = _jreq(url, {"action": "list_streams"})
    assert "del-stream" not in streams["streams"]


def test_kinesis_http_bad_action(server):
    url = server.base_url + "/kinesis"
    status, resp = _jreq(url, {"action": "explode"})
    assert status == 400
    assert resp["error"] == "ValidationException"
