"""S3-compatible object store.

A minimal but real object store: create/list/delete buckets, and
put/get/list/delete objects. Bodies are stored as BLOBs; ETags are MD5
hex digests (matching S3's single-part upload behaviour). This is a
compatible SUBSET — multipart uploads, ACLs, versioning, and presigned
URLs are roadmap items, not implemented.
"""

from __future__ import annotations

import hashlib
import re
import time
from typing import Any

from .errors import Conflict, NotFound, ValidationError
from .storage import Storage

_BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9.\-]{1,61}[a-z0-9]$")


class S3Service:
    def __init__(self, storage: Storage):
        self.storage = storage

    # --- buckets -----------------------------------------------------------
    def create_bucket(self, name: str) -> dict[str, Any]:
        if not _BUCKET_RE.match(name):
            raise ValidationError(f"invalid bucket name: {name!r}")
        existing = self.storage.query_one("SELECT name FROM s3_buckets WHERE name=?", (name,))
        if existing:
            raise Conflict(f"bucket already exists: {name}")
        self.storage.execute(
            "INSERT INTO s3_buckets(name, created_at) VALUES (?,?)", (name, time.time())
        )
        return {"name": name}

    def list_buckets(self) -> list[dict[str, Any]]:
        rows = self.storage.query("SELECT name, created_at FROM s3_buckets ORDER BY name")
        return [{"name": r["name"], "created_at": r["created_at"]} for r in rows]

    def delete_bucket(self, name: str) -> None:
        self._require_bucket(name)
        n = self.storage.query_one(
            "SELECT COUNT(*) AS c FROM s3_objects WHERE bucket=?", (name,)
        )["c"]
        if n:
            raise Conflict(f"bucket not empty: {name}")
        self.storage.execute("DELETE FROM s3_buckets WHERE name=?", (name,))

    def _require_bucket(self, name: str) -> None:
        if not self.storage.query_one("SELECT name FROM s3_buckets WHERE name=?", (name,)):
            raise NotFound(f"no such bucket: {name}")

    # --- objects -----------------------------------------------------------
    def put_object(
        self, bucket: str, key: str, body: bytes, content_type: str = "application/octet-stream"
    ) -> dict[str, Any]:
        self._require_bucket(bucket)
        if not key:
            raise ValidationError("object key must be non-empty")
        if isinstance(body, str):
            body = body.encode("utf-8")
        etag = hashlib.md5(body).hexdigest()
        now = time.time()
        self.storage.execute(
            """INSERT INTO s3_objects(bucket,key,body,content_type,etag,size,last_modified)
               VALUES (?,?,?,?,?,?,?)
               ON CONFLICT(bucket,key) DO UPDATE SET
                 body=excluded.body, content_type=excluded.content_type,
                 etag=excluded.etag, size=excluded.size, last_modified=excluded.last_modified""",
            (bucket, key, body, content_type, etag, len(body), now),
        )
        return {"bucket": bucket, "key": key, "etag": etag, "size": len(body)}

    def get_object(self, bucket: str, key: str) -> dict[str, Any]:
        self._require_bucket(bucket)
        row = self.storage.query_one(
            "SELECT * FROM s3_objects WHERE bucket=? AND key=?", (bucket, key)
        )
        if not row:
            raise NotFound(f"no such key: {key}")
        return {
            "bucket": bucket,
            "key": key,
            "body": bytes(row["body"]),
            "content_type": row["content_type"],
            "etag": row["etag"],
            "size": row["size"],
            "last_modified": row["last_modified"],
        }

    def list_objects(self, bucket: str, prefix: str = "") -> list[dict[str, Any]]:
        self._require_bucket(bucket)
        rows = self.storage.query(
            "SELECT key,etag,size,last_modified FROM s3_objects "
            "WHERE bucket=? AND key LIKE ? ORDER BY key",
            (bucket, f"{prefix}%"),
        )
        return [
            {
                "key": r["key"],
                "etag": r["etag"],
                "size": r["size"],
                "last_modified": r["last_modified"],
            }
            for r in rows
        ]

    def delete_object(self, bucket: str, key: str) -> None:
        self._require_bucket(bucket)
        self.storage.execute("DELETE FROM s3_objects WHERE bucket=? AND key=?", (bucket, key))
