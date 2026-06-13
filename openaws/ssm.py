"""SSM Parameter Store.

Implements:
  - put_parameter (String / StringList / SecureString, with KMS encryption for SecureString)
  - get_parameter / get_parameters (by name list)
  - get_parameters_by_path (hierarchy + recursive)
  - delete_parameter / delete_parameters
  - describe_parameters (with optional filters)
  - parameter history (list_parameter_history)
  - SecureString: transparent encrypt/decrypt via KMS integration when
    kms_service is wired in; falls back to storing as-is when no KMS provided.
"""

from __future__ import annotations

import json
import time
import uuid
from typing import Any

from .errors import Conflict, NotFound, ValidationError
from .storage import Storage


class SSMService:
    def __init__(self, storage: Storage, kms_service: Any | None = None):
        self.storage = storage
        self._kms = kms_service  # optional KMS integration

    # ------------------------------------------------------------------
    # Put parameter
    # ------------------------------------------------------------------

    def put_parameter(
        self,
        name: str,
        value: str,
        param_type: str = "String",
        description: str = "",
        kms_key_id: str | None = None,
        overwrite: bool = False,
        allowed_pattern: str | None = None,
        tags: list[dict] | None = None,
    ) -> dict[str, Any]:
        if not name:
            raise ValidationError("name is required")
        if param_type not in ("String", "StringList", "SecureString"):
            raise ValidationError("type must be String, StringList, or SecureString")
        if not name.startswith("/") and "/" not in name:
            # simple names without path prefix are allowed
            pass

        existing = self.storage.query_one(
            "SELECT version FROM ssm_parameters WHERE name=?", (name,)
        )
        if existing and not overwrite:
            raise Conflict(f"parameter already exists: {name}")

        # SecureString — encrypt via KMS if available
        stored_value = value
        cipher_key_id: str | None = None
        if param_type == "SecureString" and self._kms:
            if not kms_key_id:
                raise ValidationError("kms_key_id is required for SecureString")
            enc = self._kms.encrypt(kms_key_id, value.encode("utf-8"))
            stored_value = enc["ciphertext_blob"]
            cipher_key_id = enc["key_id"]

        version = (existing["version"] + 1) if existing else 1
        now = time.time()

        if existing:
            self.storage.execute(
                "UPDATE ssm_parameters SET value=?, description=?, kms_key_id=?, version=?, last_modified_at=? WHERE name=?",
                (stored_value, description, cipher_key_id, version, now, name),
            )
        else:
            self.storage.execute(
                "INSERT INTO ssm_parameters(name, value, type, description, kms_key_id, version, created_at, last_modified_at)"
                " VALUES (?,?,?,?,?,?,?,?)",
                (name, stored_value, param_type, description, cipher_key_id, version, now, now),
            )

        # store history entry
        hist_id = uuid.uuid4().hex
        self.storage.execute(
            "INSERT INTO ssm_history(id, name, value, type, version, created_at)"
            " VALUES (?,?,?,?,?,?)",
            (hist_id, name, stored_value, param_type, version, now),
        )

        if tags:
            for tag in tags:
                self._upsert_tag(name, tag["Key"], tag["Value"])

        return {"version": version, "tier": "Standard"}

    # ------------------------------------------------------------------
    # Get parameter(s)
    # ------------------------------------------------------------------

    def get_parameter(self, name: str, with_decryption: bool = True) -> dict[str, Any]:
        row = self.storage.query_one("SELECT * FROM ssm_parameters WHERE name=?", (name,))
        if not row:
            raise NotFound(f"parameter not found: {name}")
        return self._format_param(row, with_decryption)

    def get_parameters(self, names: list[str], with_decryption: bool = True) -> dict[str, Any]:
        found = []
        invalid = []
        for name in names:
            row = self.storage.query_one("SELECT * FROM ssm_parameters WHERE name=?", (name,))
            if row:
                found.append(self._format_param(row, with_decryption))
            else:
                invalid.append(name)
        return {"parameters": found, "invalid_parameters": invalid}

    def get_parameters_by_path(self, path: str, recursive: bool = False,
                                with_decryption: bool = True) -> list[dict]:
        if not path.endswith("/"):
            path = path + "/"
        if recursive:
            rows = self.storage.query(
                "SELECT * FROM ssm_parameters WHERE name LIKE ?", (path + "%",)
            )
        else:
            # only immediate children: name starts with path and has no further /
            rows = self.storage.query(
                "SELECT * FROM ssm_parameters WHERE name LIKE ?", (path + "%",)
            )
            rows = [
                r for r in rows
                if "/" not in r["name"][len(path):]
            ]
        return [self._format_param(r, with_decryption) for r in rows]

    def _format_param(self, row: Any, with_decryption: bool) -> dict[str, Any]:
        d = dict(row)
        value = d["value"]
        if d["type"] == "SecureString" and with_decryption and self._kms and d.get("kms_key_id"):
            try:
                dec = self._kms.decrypt(value)
                import base64
                value = base64.b64decode(dec["plaintext"]).decode("utf-8")
            except Exception:  # noqa: BLE001
                pass
        return {
            "name": d["name"],
            "value": value,
            "type": d["type"],
            "version": d["version"],
            "description": d.get("description", ""),
            "last_modified_at": d.get("last_modified_at"),
        }

    # ------------------------------------------------------------------
    # Delete
    # ------------------------------------------------------------------

    def delete_parameter(self, name: str) -> None:
        if not self.storage.query_one("SELECT name FROM ssm_parameters WHERE name=?", (name,)):
            raise NotFound(f"parameter not found: {name}")
        self.storage.execute("DELETE FROM ssm_parameters WHERE name=?", (name,))

    def delete_parameters(self, names: list[str]) -> dict[str, Any]:
        deleted = []
        invalid = []
        for name in names:
            row = self.storage.query_one("SELECT name FROM ssm_parameters WHERE name=?", (name,))
            if row:
                self.storage.execute("DELETE FROM ssm_parameters WHERE name=?", (name,))
                deleted.append(name)
            else:
                invalid.append(name)
        return {"deleted": deleted, "invalid": invalid}

    # ------------------------------------------------------------------
    # Describe + history
    # ------------------------------------------------------------------

    def describe_parameters(self, filters: list[dict] | None = None) -> list[dict]:
        rows = self.storage.query(
            "SELECT name, type, version, description, last_modified_at FROM ssm_parameters ORDER BY name"
        )
        result = [dict(r) for r in rows]
        if not filters:
            return result
        # apply simple key/value filters: {"Key": "Name", "Option": "Contains", "Values": [...]}
        for f in filters:
            key = f.get("Key", "")
            option = f.get("Option", "Equals")
            values = f.get("Values", [])
            filtered = []
            for p in result:
                field = p.get(key.lower().replace("-", "_"), "")
                if option == "Equals" and str(field) in values:
                    filtered.append(p)
                elif option == "Contains" and any(v in str(field) for v in values):
                    filtered.append(p)
                elif option == "BeginsWith" and any(str(field).startswith(v) for v in values):
                    filtered.append(p)
            result = filtered
        return result

    def list_parameter_history(self, name: str) -> list[dict]:
        if not self.storage.query_one("SELECT name FROM ssm_parameters WHERE name=?", (name,)):
            raise NotFound(f"parameter not found: {name}")
        rows = self.storage.query(
            "SELECT name, type, version, created_at FROM ssm_history WHERE name=? ORDER BY version",
            (name,),
        )
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Tags
    # ------------------------------------------------------------------

    def _upsert_tag(self, name: str, key: str, value: str) -> None:
        existing = self.storage.query_one(
            "SELECT 1 FROM ssm_tags WHERE param_name=? AND tag_key=?", (name, key)
        )
        if existing:
            self.storage.execute(
                "UPDATE ssm_tags SET tag_value=? WHERE param_name=? AND tag_key=?",
                (value, name, key),
            )
        else:
            self.storage.execute(
                "INSERT INTO ssm_tags(param_name, tag_key, tag_value) VALUES (?,?,?)",
                (name, key, value),
            )

    def list_tags_for_resource(self, name: str) -> list[dict]:
        rows = self.storage.query(
            "SELECT tag_key, tag_value FROM ssm_tags WHERE param_name=?", (name,)
        )
        return [{"Key": r["tag_key"], "Value": r["tag_value"]} for r in rows]
