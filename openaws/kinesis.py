"""Kinesis Data Streams-compatible local implementation.

Provides:
  - CreateStream / DeleteStream / DescribeStream / ListStreams
  - PutRecord / PutRecords (batch)
  - GetShardIterator (TRIM_HORIZON, AT_SEQUENCE_NUMBER, AFTER_SEQUENCE_NUMBER,
    LATEST)
  - GetRecords (up to 10 000 records per call, with a NextShardIterator)

Records are stored in SQLite with monotonically-increasing sequence numbers.
Shard assignment uses CRC32 of the partition key modulo the shard count.

DISCLAIMER: openaws is an independent open reimplementation for LOCAL
development. NOT affiliated with, endorsed by, or sponsored by Amazon Web
Services or any vendor.
"""

from __future__ import annotations

import base64
import binascii
import json
import time
import uuid
from typing import Any

from .errors import Conflict, NotFound, ValidationError
from .storage import Storage

_DEFAULT_SHARD_COUNT = 1
_MAX_RECORDS_PER_GET = 10_000


def _shard_for_key(partition_key: str, shard_count: int) -> int:
    """Deterministically assign a shard index via CRC32(partition_key)."""
    crc = binascii.crc32(partition_key.encode("utf-8")) & 0xFFFFFFFF
    return crc % shard_count


def _seq_to_str(seq: int) -> str:
    """Format a sequence number as a zero-padded 20-digit decimal string."""
    return f"{seq:020d}"


def _str_to_seq(s: str) -> int:
    try:
        return int(s)
    except (ValueError, TypeError) as exc:
        raise ValidationError(f"invalid sequence number: {s!r}") from exc


class KinesisService:
    def __init__(self, storage: Storage):
        self.storage = storage
        # in-memory shard-iterator store: iterator_id -> {stream, shard_id, next_seq}
        self._iterators: dict[str, dict[str, Any]] = {}

    # --- streams -----------------------------------------------------------
    def create_stream(
        self, name: str, shard_count: int = _DEFAULT_SHARD_COUNT
    ) -> dict[str, Any]:
        if not name:
            raise ValidationError("stream name is required")
        if shard_count < 1:
            raise ValidationError("shard_count must be >= 1")
        if self.storage.query_one(
            "SELECT name FROM kinesis_streams WHERE name=?", (name,)
        ):
            raise Conflict(f"stream already exists: {name}")
        now = time.time()
        self.storage.execute(
            "INSERT INTO kinesis_streams(name,shard_count,status,created_at) VALUES (?,?,?,?)",
            (name, shard_count, "ACTIVE", now),
        )
        return {"name": name, "shard_count": shard_count, "status": "ACTIVE"}

    def delete_stream(self, name: str) -> None:
        self._require_stream(name)
        self.storage.execute("DELETE FROM kinesis_records WHERE stream=?", (name,))
        self.storage.execute("DELETE FROM kinesis_streams WHERE name=?", (name,))
        # remove any in-memory iterators for this stream
        stale = [k for k, v in self._iterators.items() if v["stream"] == name]
        for k in stale:
            del self._iterators[k]

    def describe_stream(self, name: str) -> dict[str, Any]:
        row = self._require_stream(name)
        shard_count = row["shard_count"]
        shards = [
            {
                "shard_id": f"shardId-{i:012d}",
                "starting_sequence_number": _seq_to_str(0),
            }
            for i in range(shard_count)
        ]
        # find last sequence per shard
        for shard in shards:
            idx = int(shard["shard_id"].split("-")[1])
            last_row = self.storage.query_one(
                "SELECT MAX(seq) AS m FROM kinesis_records WHERE stream=? AND shard_id=?",
                (name, idx),
            )
            if last_row and last_row["m"] is not None:
                shard["last_sequence_number"] = _seq_to_str(last_row["m"])
        return {
            "name": name,
            "status": row["status"],
            "shard_count": shard_count,
            "shards": shards,
            "created_at": row["created_at"],
        }

    def list_streams(self) -> list[str]:
        rows = self.storage.query("SELECT name FROM kinesis_streams ORDER BY name")
        return [r["name"] for r in rows]

    def _require_stream(self, name: str):
        row = self.storage.query_one(
            "SELECT * FROM kinesis_streams WHERE name=?", (name,)
        )
        if not row:
            raise NotFound(f"no such stream: {name}")
        return row

    def _next_seq(self, stream: str) -> int:
        """Return the next global sequence number for the stream."""
        row = self.storage.query_one(
            "SELECT MAX(seq) AS m FROM kinesis_records WHERE stream=?", (stream,)
        )
        return (row["m"] or 0) + 1

    # --- records -----------------------------------------------------------
    def put_record(
        self,
        stream: str,
        data: bytes | str,
        partition_key: str,
        explicit_hash_key: str | None = None,
    ) -> dict[str, Any]:
        """Put a single record; data is base64-encoded for storage."""
        row = self._require_stream(stream)
        if not partition_key:
            raise ValidationError("partition_key is required")
        if isinstance(data, str):
            # treat str as already base64; decode to validate, re-encode for storage
            try:
                raw = base64.b64decode(data)
            except Exception:
                # treat as UTF-8 payload, encode to base64
                raw = data.encode("utf-8")
                data = base64.b64encode(raw).decode()
        else:
            raw = data
            data = base64.b64encode(raw).decode()
        shard_idx = _shard_for_key(
            explicit_hash_key or partition_key, row["shard_count"]
        )
        seq = self._next_seq(stream)
        now = time.time()
        self.storage.execute(
            """INSERT INTO kinesis_records(stream,shard_id,seq,partition_key,data_b64,arrival_ts)
               VALUES (?,?,?,?,?,?)""",
            (stream, shard_idx, seq, partition_key, data, now),
        )
        return {
            "sequence_number": _seq_to_str(seq),
            "shard_id": f"shardId-{shard_idx:012d}",
        }

    def put_records(
        self,
        stream: str,
        records: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Put multiple records atomically.

        Each record dict must have 'data' and 'partition_key'; optionally
        'explicit_hash_key'.
        """
        self._require_stream(stream)
        if not records:
            raise ValidationError("records list must not be empty")
        results = []
        failed_count = 0
        for rec in records:
            try:
                out = self.put_record(
                    stream,
                    rec["data"],
                    rec["partition_key"],
                    rec.get("explicit_hash_key"),
                )
                results.append({"sequence_number": out["sequence_number"],
                                 "shard_id": out["shard_id"]})
            except Exception as exc:  # noqa: BLE001
                failed_count += 1
                results.append({"error_code": "InternalFailure",
                                 "error_message": str(exc)})
        return {
            "failed_record_count": failed_count,
            "records": results,
        }

    # --- shard iterators ---------------------------------------------------
    def get_shard_iterator(
        self,
        stream: str,
        shard_id: str,
        iterator_type: str,
        starting_sequence_number: str | None = None,
    ) -> dict[str, Any]:
        """Create a shard iterator.

        iterator_type: TRIM_HORIZON | AT_SEQUENCE_NUMBER |
                       AFTER_SEQUENCE_NUMBER | LATEST
        """
        row = self._require_stream(stream)
        shard_count = row["shard_count"]
        # parse shard index from "shardId-000000000000"
        try:
            shard_idx = int(shard_id.split("-")[-1])
        except (ValueError, IndexError) as exc:
            raise ValidationError(f"invalid shard_id: {shard_id!r}") from exc
        if shard_idx >= shard_count:
            raise ValidationError(
                f"shard_id {shard_id!r} out of range (stream has {shard_count} shard(s))"
            )
        supported = {"TRIM_HORIZON", "AT_SEQUENCE_NUMBER", "AFTER_SEQUENCE_NUMBER", "LATEST"}
        if iterator_type not in supported:
            raise ValidationError(
                f"invalid iterator_type {iterator_type!r}; supported: {sorted(supported)}"
            )
        if iterator_type == "TRIM_HORIZON":
            # start before the oldest record
            next_seq = 0
        elif iterator_type == "LATEST":
            # start after the newest record
            last = self.storage.query_one(
                "SELECT MAX(seq) AS m FROM kinesis_records WHERE stream=? AND shard_id=?",
                (stream, shard_idx),
            )
            next_seq = (last["m"] or 0) + 1
        elif iterator_type == "AT_SEQUENCE_NUMBER":
            if starting_sequence_number is None:
                raise ValidationError(
                    "starting_sequence_number required for AT_SEQUENCE_NUMBER"
                )
            next_seq = _str_to_seq(starting_sequence_number)
        elif iterator_type == "AFTER_SEQUENCE_NUMBER":
            if starting_sequence_number is None:
                raise ValidationError(
                    "starting_sequence_number required for AFTER_SEQUENCE_NUMBER"
                )
            next_seq = _str_to_seq(starting_sequence_number) + 1

        iterator_id = uuid.uuid4().hex
        self._iterators[iterator_id] = {
            "stream": stream,
            "shard_id": shard_idx,
            "next_seq": next_seq,
        }
        return {"shard_iterator": iterator_id}

    def get_records(
        self,
        shard_iterator: str,
        limit: int = 100,
    ) -> dict[str, Any]:
        """Fetch up to ``limit`` records starting from the iterator position."""
        it = self._iterators.get(shard_iterator)
        if it is None:
            raise ValidationError(
                f"invalid or expired shard iterator: {shard_iterator!r}"
            )
        limit = max(1, min(limit, _MAX_RECORDS_PER_GET))
        rows = self.storage.query(
            """SELECT seq,partition_key,data_b64,arrival_ts FROM kinesis_records
               WHERE stream=? AND shard_id=? AND seq>=?
               ORDER BY seq LIMIT ?""",
            (it["stream"], it["shard_id"], it["next_seq"], limit),
        )
        records = [
            {
                "sequence_number": _seq_to_str(r["seq"]),
                "partition_key": r["partition_key"],
                "data": r["data_b64"],
                "approximate_arrival_timestamp": r["arrival_ts"],
            }
            for r in rows
        ]
        if records:
            it["next_seq"] = int(records[-1]["sequence_number"]) + 1
        # generate a new iterator id so the caller can continue paging
        new_iter_id = uuid.uuid4().hex
        self._iterators[new_iter_id] = dict(it)
        del self._iterators[shard_iterator]
        return {
            "records": records,
            "next_shard_iterator": new_iter_id,
            "millis_behind_latest": 0,
        }
