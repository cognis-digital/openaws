"""S3-compatible object store.

A real object store: create/list/delete buckets, put/get/list/delete objects.
Bodies are stored as BLOBs; ETags are MD5 hex digests (matching S3's single-part
upload behaviour).

This pass adds:
  - Multipart upload (CreateMultipartUpload / UploadPart / CompleteMultipartUpload /
    AbortMultipartUpload / ListParts)
  - Object versioning (enable/suspend per bucket; GET returns latest; GET?versionId=...
    returns specific version; ListObjectVersions)
  - Object tagging (PutObjectTagging / GetObjectTagging / DeleteObjectTagging)
  - Object copy (CopyObject from source bucket/key to dest bucket/key)
  - Prefix + delimiter listing (common-prefix / "folder" simulation)
  - Per-object metadata (arbitrary x-amz-meta-* stored as JSON)
  - Presigned-URL token stub (generates a signed token verifiable locally; does NOT
    require network — the token is a HMAC-SHA256 over key material)
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import re
import time
import uuid
from typing import Any

from .errors import Conflict, NotFound, ValidationError
from .storage import Storage

_BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9.\-]{1,61}[a-z0-9]$")

# Secret used for presigned-URL HMAC tokens — in a real deployment this would be
# derived from credentials; here we use a fixed local key for deterministic testing.
_PRESIGN_SECRET = b"openaws-local-presign-secret-v1"


def _md5(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


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
            "INSERT INTO s3_buckets(name, created_at, versioning) VALUES (?,?,?)",
            (name, time.time(), "disabled"),
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

    def _require_bucket(self, name: str):
        row = self.storage.query_one("SELECT * FROM s3_buckets WHERE name=?", (name,))
        if not row:
            raise NotFound(f"no such bucket: {name}")
        return row

    # --- versioning --------------------------------------------------------
    def put_bucket_versioning(self, bucket: str, status: str) -> None:
        """Enable or suspend versioning. status must be 'enabled' or 'suspended'."""
        self._require_bucket(bucket)
        if status not in ("enabled", "suspended"):
            raise ValidationError("versioning status must be 'enabled' or 'suspended'")
        self.storage.execute(
            "UPDATE s3_buckets SET versioning=? WHERE name=?", (status, bucket)
        )

    def get_bucket_versioning(self, bucket: str) -> dict[str, Any]:
        row = self._require_bucket(bucket)
        return {"bucket": bucket, "versioning": row["versioning"]}

    # --- objects -----------------------------------------------------------
    def put_object(
        self,
        bucket: str,
        key: str,
        body: bytes,
        content_type: str = "application/octet-stream",
        metadata: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        self._require_bucket(bucket)
        if not key:
            raise ValidationError("object key must be non-empty")
        if isinstance(body, str):
            body = body.encode("utf-8")
        bucket_row = self._require_bucket(bucket)
        etag = _md5(body)
        now = time.time()
        version_id = uuid.uuid4().hex if bucket_row["versioning"] == "enabled" else None
        meta_json = json.dumps(metadata or {})
        if version_id:
            # versioning enabled: insert a new version row; mark all prior as not-latest
            self.storage.execute(
                "UPDATE s3_object_versions SET is_latest=0 WHERE bucket=? AND key=?",
                (bucket, key),
            )
            self.storage.execute(
                """INSERT INTO s3_object_versions
                   (bucket,key,version_id,body,content_type,etag,size,last_modified,is_latest,meta_json)
                   VALUES (?,?,?,?,?,?,?,?,1,?)""",
                (bucket, key, version_id, body, content_type, etag, len(body), now, meta_json),
            )
            # keep s3_objects as the "latest" pointer
            self.storage.execute(
                """INSERT INTO s3_objects(bucket,key,body,content_type,etag,size,last_modified,meta_json)
                   VALUES (?,?,?,?,?,?,?,?)
                   ON CONFLICT(bucket,key) DO UPDATE SET
                     body=excluded.body, content_type=excluded.content_type,
                     etag=excluded.etag, size=excluded.size,
                     last_modified=excluded.last_modified, meta_json=excluded.meta_json""",
                (bucket, key, body, content_type, etag, len(body), now, meta_json),
            )
        else:
            self.storage.execute(
                """INSERT INTO s3_objects(bucket,key,body,content_type,etag,size,last_modified,meta_json)
                   VALUES (?,?,?,?,?,?,?,?)
                   ON CONFLICT(bucket,key) DO UPDATE SET
                     body=excluded.body, content_type=excluded.content_type,
                     etag=excluded.etag, size=excluded.size,
                     last_modified=excluded.last_modified, meta_json=excluded.meta_json""",
                (bucket, key, body, content_type, etag, len(body), now, meta_json),
            )
        result: dict[str, Any] = {
            "bucket": bucket, "key": key, "etag": etag, "size": len(body),
        }
        if version_id:
            result["version_id"] = version_id
        return result

    def get_object(
        self, bucket: str, key: str, version_id: str | None = None
    ) -> dict[str, Any]:
        self._require_bucket(bucket)
        if version_id:
            row = self.storage.query_one(
                "SELECT * FROM s3_object_versions WHERE bucket=? AND key=? AND version_id=?",
                (bucket, key, version_id),
            )
            if not row:
                raise NotFound(f"no such key/version: {key}/{version_id}")
        else:
            row = self.storage.query_one(
                "SELECT * FROM s3_objects WHERE bucket=? AND key=?", (bucket, key)
            )
            if not row:
                raise NotFound(f"no such key: {key}")
        meta = json.loads(row["meta_json"]) if row["meta_json"] else {}
        result: dict[str, Any] = {
            "bucket": bucket,
            "key": key,
            "body": bytes(row["body"]),
            "content_type": row["content_type"],
            "etag": row["etag"],
            "size": row["size"],
            "last_modified": row["last_modified"],
            "metadata": meta,
        }
        if "version_id" in row.keys():
            result["version_id"] = row["version_id"]
        return result

    def list_objects(
        self,
        bucket: str,
        prefix: str = "",
        delimiter: str = "",
        max_keys: int = 1000,
    ) -> dict[str, Any]:
        """List objects; supports prefix filtering and delimiter-based folding.

        Returns a dict with keys 'objects' (list of object dicts) and
        'common_prefixes' (list of prefix strings when delimiter is set).
        """
        self._require_bucket(bucket)
        rows = self.storage.query(
            "SELECT key,etag,size,last_modified FROM s3_objects "
            "WHERE bucket=? AND key LIKE ? ORDER BY key",
            (bucket, f"{prefix}%"),
        )
        objects = []
        common_prefixes: set[str] = set()
        for r in rows:
            key = r["key"]
            if delimiter:
                # find the delimiter after the prefix
                suffix = key[len(prefix):]
                idx = suffix.find(delimiter)
                if idx >= 0:
                    cp = prefix + suffix[: idx + len(delimiter)]
                    common_prefixes.add(cp)
                    continue
            objects.append(
                {
                    "key": key,
                    "etag": r["etag"],
                    "size": r["size"],
                    "last_modified": r["last_modified"],
                }
            )
            if len(objects) >= max_keys:
                break
        return {
            "objects": objects,
            "common_prefixes": sorted(common_prefixes),
        }

    def list_object_versions(self, bucket: str, prefix: str = "") -> dict[str, Any]:
        """List all versions of all objects (requires versioning-enabled bucket)."""
        self._require_bucket(bucket)
        rows = self.storage.query(
            """SELECT key,version_id,etag,size,last_modified,is_latest
               FROM s3_object_versions
               WHERE bucket=? AND key LIKE ?
               ORDER BY key, last_modified DESC""",
            (bucket, f"{prefix}%"),
        )
        return {
            "versions": [
                {
                    "key": r["key"],
                    "version_id": r["version_id"],
                    "etag": r["etag"],
                    "size": r["size"],
                    "last_modified": r["last_modified"],
                    "is_latest": bool(r["is_latest"]),
                }
                for r in rows
            ]
        }

    def delete_object(
        self, bucket: str, key: str, version_id: str | None = None
    ) -> None:
        self._require_bucket(bucket)
        if version_id:
            self.storage.execute(
                "DELETE FROM s3_object_versions WHERE bucket=? AND key=? AND version_id=?",
                (bucket, key, version_id),
            )
            # if we deleted the latest version, promote the next
            next_ver = self.storage.query_one(
                "SELECT version_id FROM s3_object_versions WHERE bucket=? AND key=? "
                "ORDER BY last_modified DESC LIMIT 1",
                (bucket, key),
            )
            if next_ver:
                self.storage.execute(
                    "UPDATE s3_object_versions SET is_latest=1 WHERE bucket=? AND key=? AND version_id=?",
                    (bucket, key, next_ver["version_id"]),
                )
            else:
                # no more versions — also delete latest pointer
                self.storage.execute(
                    "DELETE FROM s3_objects WHERE bucket=? AND key=?", (bucket, key)
                )
        else:
            self.storage.execute(
                "DELETE FROM s3_objects WHERE bucket=? AND key=?", (bucket, key)
            )
            # also wipe all versions if versioning was not enabled (no version rows anyway)
            self.storage.execute(
                "DELETE FROM s3_object_versions WHERE bucket=? AND key=?", (bucket, key)
            )

    # --- object tagging ----------------------------------------------------
    def put_object_tagging(
        self, bucket: str, key: str, tags: dict[str, str]
    ) -> None:
        self._require_bucket(bucket)
        self._require_object(bucket, key)
        if not isinstance(tags, dict):
            raise ValidationError("tags must be a dict")
        # upsert per tag
        self.storage.execute(
            "DELETE FROM s3_object_tags WHERE bucket=? AND key=?", (bucket, key)
        )
        for k, v in tags.items():
            self.storage.execute(
                "INSERT INTO s3_object_tags(bucket,key,tag_key,tag_value) VALUES (?,?,?,?)",
                (bucket, key, str(k), str(v)),
            )

    def get_object_tagging(self, bucket: str, key: str) -> dict[str, str]:
        self._require_bucket(bucket)
        self._require_object(bucket, key)
        rows = self.storage.query(
            "SELECT tag_key, tag_value FROM s3_object_tags WHERE bucket=? AND key=?",
            (bucket, key),
        )
        return {r["tag_key"]: r["tag_value"] for r in rows}

    def delete_object_tagging(self, bucket: str, key: str) -> None:
        self._require_bucket(bucket)
        self._require_object(bucket, key)
        self.storage.execute(
            "DELETE FROM s3_object_tags WHERE bucket=? AND key=?", (bucket, key)
        )

    def _require_object(self, bucket: str, key: str):
        row = self.storage.query_one(
            "SELECT key FROM s3_objects WHERE bucket=? AND key=?", (bucket, key)
        )
        if not row:
            raise NotFound(f"no such key: {key}")
        return row

    # --- copy object -------------------------------------------------------
    def copy_object(
        self,
        src_bucket: str,
        src_key: str,
        dst_bucket: str,
        dst_key: str,
        metadata: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Copy an object from src_bucket/src_key to dst_bucket/dst_key."""
        src = self.get_object(src_bucket, src_key)
        meta = metadata if metadata is not None else src.get("metadata", {})
        return self.put_object(
            dst_bucket,
            dst_key,
            src["body"],
            src["content_type"],
            meta,
        )

    # --- multipart upload --------------------------------------------------
    def create_multipart_upload(
        self,
        bucket: str,
        key: str,
        content_type: str = "application/octet-stream",
        metadata: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        self._require_bucket(bucket)
        if not key:
            raise ValidationError("object key must be non-empty")
        upload_id = uuid.uuid4().hex
        now = time.time()
        meta_json = json.dumps(metadata or {})
        self.storage.execute(
            """INSERT INTO s3_multipart_uploads
               (upload_id,bucket,key,content_type,meta_json,created_at)
               VALUES (?,?,?,?,?,?)""",
            (upload_id, bucket, key, content_type, meta_json, now),
        )
        return {"upload_id": upload_id, "bucket": bucket, "key": key}

    def upload_part(
        self,
        bucket: str,
        key: str,
        upload_id: str,
        part_number: int,
        body: bytes,
    ) -> dict[str, Any]:
        self._require_multipart(upload_id, bucket, key)
        if not (1 <= part_number <= 10000):
            raise ValidationError("part_number must be between 1 and 10000")
        if isinstance(body, str):
            body = body.encode("utf-8")
        etag = _md5(body)
        self.storage.execute(
            """INSERT INTO s3_multipart_parts
               (upload_id,bucket,key,part_number,body,etag,size)
               VALUES (?,?,?,?,?,?,?)
               ON CONFLICT(upload_id,part_number) DO UPDATE SET
                 body=excluded.body, etag=excluded.etag, size=excluded.size""",
            (upload_id, bucket, key, part_number, body, etag, len(body)),
        )
        return {"part_number": part_number, "etag": etag}

    def complete_multipart_upload(
        self,
        bucket: str,
        key: str,
        upload_id: str,
        parts: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Combine parts in order and store the final object."""
        upload_row = self._require_multipart(upload_id, bucket, key)
        part_rows = self.storage.query(
            "SELECT part_number,body,etag FROM s3_multipart_parts "
            "WHERE upload_id=? ORDER BY part_number",
            (upload_id,),
        )
        # build a part_number -> row index map
        stored = {r["part_number"]: r for r in part_rows}
        # validate the caller-supplied part list
        ordered_bodies = []
        etag_parts = []
        for p in sorted(parts, key=lambda x: x["part_number"]):
            pn = p["part_number"]
            if pn not in stored:
                raise ValidationError(f"part {pn} not uploaded")
            ordered_bodies.append(bytes(stored[pn]["body"]))
            etag_parts.append(stored[pn]["etag"])
        final_body = b"".join(ordered_bodies)
        # multipart etag: md5 of concatenated part etags (as bytes), suffixed with -<n>
        combined_md5 = hashlib.md5(
            b"".join(bytes.fromhex(e) for e in etag_parts)
        ).hexdigest()
        final_etag = f"{combined_md5}-{len(parts)}"
        meta = json.loads(upload_row["meta_json"]) if upload_row["meta_json"] else {}
        result = self.put_object(
            bucket, key, final_body, upload_row["content_type"], meta
        )
        result["etag"] = final_etag  # override with multipart etag
        # clean up
        self.storage.execute(
            "DELETE FROM s3_multipart_parts WHERE upload_id=?", (upload_id,)
        )
        self.storage.execute(
            "DELETE FROM s3_multipart_uploads WHERE upload_id=?", (upload_id,)
        )
        return result

    def abort_multipart_upload(
        self, bucket: str, key: str, upload_id: str
    ) -> None:
        self._require_multipart(upload_id, bucket, key)
        self.storage.execute(
            "DELETE FROM s3_multipart_parts WHERE upload_id=?", (upload_id,)
        )
        self.storage.execute(
            "DELETE FROM s3_multipart_uploads WHERE upload_id=?", (upload_id,)
        )

    def list_parts(
        self, bucket: str, key: str, upload_id: str
    ) -> list[dict[str, Any]]:
        self._require_multipart(upload_id, bucket, key)
        rows = self.storage.query(
            "SELECT part_number,etag,size FROM s3_multipart_parts "
            "WHERE upload_id=? ORDER BY part_number",
            (upload_id,),
        )
        return [
            {"part_number": r["part_number"], "etag": r["etag"], "size": r["size"]}
            for r in rows
        ]

    def _require_multipart(self, upload_id: str, bucket: str, key: str):
        row = self.storage.query_one(
            "SELECT * FROM s3_multipart_uploads WHERE upload_id=? AND bucket=? AND key=?",
            (upload_id, bucket, key),
        )
        if not row:
            raise NotFound(f"no such multipart upload: {upload_id}")
        return row

    # --- presigned URL token stub ------------------------------------------
    def generate_presigned_url(
        self,
        bucket: str,
        key: str,
        operation: str = "get_object",
        expires_in: int = 3600,
    ) -> dict[str, Any]:
        """Generate a presigned URL token for local verification.

        The returned token is a HMAC-SHA256 signature over
        ``{bucket}/{key}/{operation}/{expires_at}``. To validate, call
        ``verify_presigned_token``. The URL itself is a localhost stub.
        """
        self._require_bucket(bucket)
        expires_at = int(time.time()) + expires_in
        payload = f"{bucket}/{key}/{operation}/{expires_at}"
        sig = hmac.new(_PRESIGN_SECRET, payload.encode(), hashlib.sha256).hexdigest()
        token = base64.urlsafe_b64encode(
            json.dumps(
                {"bucket": bucket, "key": key, "op": operation,
                 "exp": expires_at, "sig": sig}
            ).encode()
        ).decode()
        return {
            "url": f"http://localhost:4566/s3/{bucket}/{key}?X-OpenAWS-Token={token}",
            "token": token,
            "expires_at": expires_at,
            "operation": operation,
        }

    def verify_presigned_token(self, token: str) -> dict[str, Any]:
        """Verify a token returned by generate_presigned_url."""
        try:
            data = json.loads(base64.urlsafe_b64decode(token.encode()))
        except Exception as exc:
            raise ValidationError(f"invalid presigned token: {exc}") from exc
        if int(time.time()) > data["exp"]:
            raise ValidationError("presigned token has expired")
        payload = f"{data['bucket']}/{data['key']}/{data['op']}/{data['exp']}"
        expected_sig = hmac.new(
            _PRESIGN_SECRET, payload.encode(), hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(expected_sig, data["sig"]):
            raise ValidationError("presigned token signature invalid")
        return {
            "bucket": data["bucket"],
            "key": data["key"],
            "operation": data["op"],
            "expires_at": data["exp"],
            "valid": True,
        }
