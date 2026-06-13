"""KMS — Key Management Service.

Implements:
  - Customer Master Keys (CMKs): create/list/describe/schedule_key_deletion/disable/enable
  - encrypt / decrypt (AES-256-GCM via XOR-based stdlib simulation, no third-party deps)
  - generate_data_key / generate_data_key_without_plaintext
  - key aliases: create/list/delete
  - key rotation (enable/disable/status)

Crypto note: real AES-GCM requires a third-party library.  openaws uses a
deterministic, HMAC-SHA256-based stream cipher that is structurally equivalent
(same key/IV derivation pattern) so that encrypt→decrypt round-trips work
correctly in tests.  Do NOT use this for real data protection.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
import uuid
from typing import Any

from .errors import Conflict, NotFound, ValidationError
from .storage import Storage

_ARN_PREFIX = "arn:openaws:kms:local:000:key/"


def _arn(key_id: str) -> str:
    return f"{_ARN_PREFIX}{key_id}"


def _xor_bytes(a: bytes, b: bytes) -> bytes:
    return bytes(x ^ y for x, y in zip(a, b))


def _derive_keystream(key_bytes: bytes, iv: bytes, length: int) -> bytes:
    """Deterministic keystream via repeated HMAC-SHA256 block generation."""
    stream = b""
    counter = 0
    while len(stream) < length:
        block = hmac.new(key_bytes, iv + counter.to_bytes(4, "big"), hashlib.sha256).digest()
        stream += block
        counter += 1
    return stream[:length]


def _encrypt_bytes(key_bytes: bytes, plaintext: bytes) -> bytes:
    """Encrypt with a random 16-byte IV prepended; append 32-byte HMAC tag."""
    iv = os.urandom(16)
    keystream = _derive_keystream(key_bytes, iv, len(plaintext))
    ciphertext = _xor_bytes(plaintext, keystream)
    tag = hmac.new(key_bytes, iv + ciphertext, hashlib.sha256).digest()
    return iv + ciphertext + tag


def _decrypt_bytes(key_bytes: bytes, blob: bytes) -> bytes:
    """Decrypt blob produced by _encrypt_bytes.  Raises ValueError on tag mismatch."""
    if len(blob) < 16 + 32:
        raise ValueError("ciphertext too short")
    iv = blob[:16]
    ciphertext = blob[16:-32]
    tag = blob[-32:]
    expected = hmac.new(key_bytes, iv + ciphertext, hashlib.sha256).digest()
    if not hmac.compare_digest(tag, expected):
        raise ValueError("authentication tag mismatch")
    keystream = _derive_keystream(key_bytes, iv, len(ciphertext))
    return _xor_bytes(ciphertext, keystream)


class KMSService:
    def __init__(self, storage: Storage):
        self.storage = storage

    # ------------------------------------------------------------------
    # Key management
    # ------------------------------------------------------------------

    def create_key(self, description: str = "", key_usage: str = "ENCRYPT_DECRYPT",
                   key_spec: str = "SYMMETRIC_DEFAULT") -> dict[str, Any]:
        key_id = uuid.uuid4().hex
        arn = _arn(key_id)
        # Generate a random 32-byte key material and store it encoded
        key_material = base64.b64encode(os.urandom(32)).decode()
        now = time.time()
        self.storage.execute(
            "INSERT INTO kms_keys(key_id, arn, description, key_usage, key_spec,"
            " key_material_b64, state, rotation_enabled, created_at)"
            " VALUES (?,?,?,?,?,?,?,?,?)",
            (key_id, arn, description, key_usage, key_spec,
             key_material, "Enabled", 0, now),
        )
        return {"key_id": key_id, "arn": arn, "description": description,
                "key_usage": key_usage, "key_spec": key_spec, "state": "Enabled"}

    def describe_key(self, key_id: str) -> dict[str, Any]:
        row = self._require_key(key_id)
        d = dict(row)
        d.pop("key_material_b64", None)
        return d

    def list_keys(self) -> list[dict]:
        rows = self.storage.query(
            "SELECT key_id, arn, description, state FROM kms_keys ORDER BY created_at"
        )
        return [dict(r) for r in rows]

    def schedule_key_deletion(self, key_id: str, pending_window_in_days: int = 30) -> dict:
        self._require_key(key_id)
        self.storage.execute(
            "UPDATE kms_keys SET state='PendingDeletion' WHERE key_id=?", (key_id,)
        )
        return {"key_id": key_id, "state": "PendingDeletion",
                "deletion_date": time.time() + pending_window_in_days * 86400}

    def disable_key(self, key_id: str) -> None:
        self._require_key(key_id)
        self.storage.execute("UPDATE kms_keys SET state='Disabled' WHERE key_id=?", (key_id,))

    def enable_key(self, key_id: str) -> None:
        self._require_key(key_id)
        self.storage.execute("UPDATE kms_keys SET state='Enabled' WHERE key_id=?", (key_id,))

    def _require_key(self, key_id: str) -> Any:
        row = self.storage.query_one("SELECT * FROM kms_keys WHERE key_id=? OR arn=?", (key_id, key_id))
        if not row:
            raise NotFound(f"no such KMS key: {key_id}")
        return row

    def _key_bytes(self, key_id: str) -> bytes:
        row = self._require_key(key_id)
        d = dict(row)
        if d["state"] != "Enabled":
            raise ValidationError(f"key {key_id} is not enabled (state={d['state']})")
        return base64.b64decode(d["key_material_b64"])

    # ------------------------------------------------------------------
    # Encrypt / Decrypt
    # ------------------------------------------------------------------

    def encrypt(self, key_id: str, plaintext: bytes | str,
                encryption_context: dict | None = None) -> dict[str, Any]:
        if isinstance(plaintext, str):
            plaintext = plaintext.encode("utf-8")
        key_bytes = self._key_bytes(key_id)
        row = self._require_key(key_id)
        real_key_id = dict(row)["key_id"]
        # Mix in encryption context via HMAC
        ctx_bytes = json.dumps(encryption_context or {}, sort_keys=True).encode()
        derived = hmac.new(key_bytes, ctx_bytes, hashlib.sha256).digest()
        cipherblob = _encrypt_bytes(derived, plaintext)
        # Wrap: key_id + base64 ciphertext
        payload = json.dumps({
            "k": real_key_id,
            "c": base64.b64encode(cipherblob).decode(),
        }).encode()
        ciphertext_blob = base64.b64encode(payload).decode()
        return {
            "ciphertext_blob": ciphertext_blob,
            "key_id": real_key_id,
            "key_arn": _arn(real_key_id),
        }

    def decrypt(self, ciphertext_blob: str,
                encryption_context: dict | None = None) -> dict[str, Any]:
        try:
            payload = json.loads(base64.b64decode(ciphertext_blob).decode())
            key_id = payload["k"]
            cipherblob = base64.b64decode(payload["c"])
        except Exception as exc:
            raise ValidationError(f"invalid ciphertext_blob: {exc}") from exc
        key_bytes = self._key_bytes(key_id)
        ctx_bytes = json.dumps(encryption_context or {}, sort_keys=True).encode()
        derived = hmac.new(key_bytes, ctx_bytes, hashlib.sha256).digest()
        plaintext = _decrypt_bytes(derived, cipherblob)
        return {
            "plaintext": base64.b64encode(plaintext).decode(),
            "key_id": key_id,
            "key_arn": _arn(key_id),
        }

    # ------------------------------------------------------------------
    # GenerateDataKey
    # ------------------------------------------------------------------

    def generate_data_key(self, key_id: str, key_spec: str = "AES_256",
                          number_of_bytes: int | None = None,
                          encryption_context: dict | None = None) -> dict[str, Any]:
        key_bytes = self._key_bytes(key_id)
        row = self._require_key(key_id)
        real_key_id = dict(row)["key_id"]
        length = number_of_bytes or (32 if key_spec in ("AES_256", "") else 16)
        plaintext_key = os.urandom(length)
        encrypted = self.encrypt(real_key_id, plaintext_key, encryption_context)
        return {
            "plaintext": base64.b64encode(plaintext_key).decode(),
            "ciphertext_blob": encrypted["ciphertext_blob"],
            "key_id": real_key_id,
        }

    def generate_data_key_without_plaintext(self, key_id: str, key_spec: str = "AES_256",
                                             number_of_bytes: int | None = None,
                                             encryption_context: dict | None = None) -> dict[str, Any]:
        res = self.generate_data_key(key_id, key_spec, number_of_bytes, encryption_context)
        # strip the plaintext
        return {"ciphertext_blob": res["ciphertext_blob"], "key_id": res["key_id"]}

    # ------------------------------------------------------------------
    # Key aliases
    # ------------------------------------------------------------------

    def create_alias(self, alias_name: str, target_key_id: str) -> None:
        if not alias_name.startswith("alias/"):
            raise ValidationError("alias_name must start with 'alias/'")
        row = self._require_key(target_key_id)
        real_key_id = dict(row)["key_id"]
        if self.storage.query_one("SELECT 1 FROM kms_aliases WHERE alias_name=?", (alias_name,)):
            raise Conflict(f"alias already exists: {alias_name}")
        self.storage.execute(
            "INSERT INTO kms_aliases(alias_name, target_key_id, created_at) VALUES (?,?,?)",
            (alias_name, real_key_id, time.time()),
        )

    def list_aliases(self) -> list[dict]:
        rows = self.storage.query(
            "SELECT alias_name, target_key_id FROM kms_aliases ORDER BY alias_name"
        )
        return [dict(r) for r in rows]

    def delete_alias(self, alias_name: str) -> None:
        self.storage.execute("DELETE FROM kms_aliases WHERE alias_name=?", (alias_name,))

    # ------------------------------------------------------------------
    # Key rotation
    # ------------------------------------------------------------------

    def enable_key_rotation(self, key_id: str) -> None:
        self._require_key(key_id)
        self.storage.execute("UPDATE kms_keys SET rotation_enabled=1 WHERE key_id=?", (key_id,))

    def disable_key_rotation(self, key_id: str) -> None:
        self._require_key(key_id)
        self.storage.execute("UPDATE kms_keys SET rotation_enabled=0 WHERE key_id=?", (key_id,))

    def get_key_rotation_status(self, key_id: str) -> dict:
        row = self._require_key(key_id)
        return {"key_id": dict(row)["key_id"],
                "key_rotation_enabled": bool(dict(row)["rotation_enabled"])}
