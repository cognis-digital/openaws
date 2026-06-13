"""Secrets Manager.

Implements:
  - create_secret / update_secret / delete_secret / list_secrets / describe_secret
  - get_secret_value (current or by version-id / version-stage)
  - put_secret_value (adds a new version)
  - restore_secret (undo scheduled deletion)
  - tag_resource / untag_resource / list_tags_for_resource
  - rotation stub: rotate_secret stores a rotation configuration; actual
    rotation logic is left to the caller's Lambda (rotation_lambda_arn stored
    but not invoked — consistent with the "stub" requirement)
"""

from __future__ import annotations

import json
import time
import uuid
from typing import Any

from .errors import Conflict, NotFound, ValidationError
from .storage import Storage

_ARN_PREFIX = "arn:openaws:secretsmanager:local:000:secret:"


def _arn(name: str) -> str:
    return f"{_ARN_PREFIX}{name}"


class SecretsManagerService:
    def __init__(self, storage: Storage):
        self.storage = storage

    # ------------------------------------------------------------------
    # Secrets CRUD
    # ------------------------------------------------------------------

    def create_secret(self, name: str, secret_string: str | None = None,
                      secret_binary: bytes | None = None,
                      description: str = "",
                      tags: list[dict] | None = None) -> dict[str, Any]:
        if not name:
            raise ValidationError("name is required")
        if self.storage.query_one("SELECT name FROM sm_secrets WHERE name=?", (name,)):
            raise Conflict(f"secret already exists: {name}")
        if secret_string is None and secret_binary is None:
            raise ValidationError("either secret_string or secret_binary is required")
        arn = _arn(name)
        now = time.time()
        version_id = uuid.uuid4().hex
        binary_b64: str | None = None
        if secret_binary is not None:
            import base64
            binary_b64 = base64.b64encode(secret_binary).decode()
        self.storage.execute(
            "INSERT INTO sm_secrets(name, arn, description, rotation_enabled,"
            " rotation_lambda_arn, rotation_rules_json, deleted_at, created_at)"
            " VALUES (?,?,?,?,?,?,?,?)",
            (name, arn, description, 0, None, None, None, now),
        )
        self.storage.execute(
            "INSERT INTO sm_versions(version_id, secret_name, secret_string, secret_binary_b64,"
            " stages_json, created_at)"
            " VALUES (?,?,?,?,?,?)",
            (version_id, name, secret_string, binary_b64,
             json.dumps(["AWSCURRENT"]), now),
        )
        if tags:
            for tag in tags:
                self._upsert_tag(name, tag["Key"], tag["Value"])
        return {"name": name, "arn": arn, "version_id": version_id}

    def describe_secret(self, name: str) -> dict[str, Any]:
        row = self._require_secret(name)
        d = dict(row)
        d.pop("rotation_rules_json", None)
        rotation_rules = row["rotation_rules_json"]
        d["rotation_rules"] = json.loads(rotation_rules) if rotation_rules else None
        return d

    def list_secrets(self) -> list[dict]:
        rows = self.storage.query(
            "SELECT name, arn, description, rotation_enabled, deleted_at FROM sm_secrets ORDER BY name"
        )
        return [dict(r) for r in rows]

    def update_secret(self, name: str, description: str | None = None,
                      secret_string: str | None = None,
                      secret_binary: bytes | None = None) -> dict[str, Any]:
        row = self._require_secret(name)
        if row["deleted_at"] is not None:
            raise ValidationError(f"secret {name} is scheduled for deletion")
        if description is not None:
            self.storage.execute("UPDATE sm_secrets SET description=? WHERE name=?", (description, name))
        if secret_string is not None or secret_binary is not None:
            return self.put_secret_value(name, secret_string=secret_string,
                                          secret_binary=secret_binary)
        return {"name": name, "arn": dict(row)["arn"]}

    def delete_secret(self, name: str, recovery_window_in_days: int = 30,
                      force_delete_without_recovery: bool = False) -> dict[str, Any]:
        row = self._require_secret(name)
        if force_delete_without_recovery:
            self.storage.execute("DELETE FROM sm_versions WHERE secret_name=?", (name,))
            self.storage.execute("DELETE FROM sm_tags WHERE secret_name=?", (name,))
            self.storage.execute("DELETE FROM sm_secrets WHERE name=?", (name,))
            return {"name": name, "deleted": True}
        deletion_date = time.time() + recovery_window_in_days * 86400
        self.storage.execute(
            "UPDATE sm_secrets SET deleted_at=? WHERE name=?", (deletion_date, name)
        )
        return {"name": name, "arn": dict(row)["arn"],
                "deletion_date": deletion_date}

    def restore_secret(self, name: str) -> dict[str, Any]:
        row = self._require_secret(name)
        self.storage.execute("UPDATE sm_secrets SET deleted_at=NULL WHERE name=?", (name,))
        return {"name": name, "arn": dict(row)["arn"]}

    # ------------------------------------------------------------------
    # Versions
    # ------------------------------------------------------------------

    def put_secret_value(self, name: str, secret_string: str | None = None,
                          secret_binary: bytes | None = None,
                          client_request_token: str | None = None,
                          version_stages: list[str] | None = None) -> dict[str, Any]:
        row = self._require_secret(name)
        if row["deleted_at"] is not None:
            raise ValidationError(f"secret {name} is scheduled for deletion")
        if secret_string is None and secret_binary is None:
            raise ValidationError("either secret_string or secret_binary is required")
        import base64
        binary_b64: str | None = None
        if secret_binary is not None:
            binary_b64 = base64.b64encode(secret_binary).decode()
        version_id = client_request_token or uuid.uuid4().hex
        stages = version_stages or ["AWSCURRENT"]
        now = time.time()
        # demote AWSCURRENT on existing version
        if "AWSCURRENT" in stages:
            old = self.storage.query(
                "SELECT version_id, stages_json FROM sm_versions WHERE secret_name=?", (name,)
            )
            for o in old:
                old_stages = json.loads(o["stages_json"] or "[]")
                if "AWSCURRENT" in old_stages:
                    old_stages = [s for s in old_stages if s != "AWSCURRENT"]
                    if "AWSPREVIOUS" not in old_stages:
                        old_stages.append("AWSPREVIOUS")
                    self.storage.execute(
                        "UPDATE sm_versions SET stages_json=? WHERE version_id=?",
                        (json.dumps(old_stages), o["version_id"]),
                    )
        self.storage.execute(
            "INSERT OR REPLACE INTO sm_versions"
            "(version_id, secret_name, secret_string, secret_binary_b64, stages_json, created_at)"
            " VALUES (?,?,?,?,?,?)",
            (version_id, name, secret_string, binary_b64, json.dumps(stages), now),
        )
        return {"name": name, "arn": dict(row)["arn"],
                "version_id": version_id, "version_stages": stages}

    def get_secret_value(self, name: str, version_id: str | None = None,
                          version_stage: str = "AWSCURRENT") -> dict[str, Any]:
        self._require_secret(name)
        if version_id:
            row = self.storage.query_one(
                "SELECT * FROM sm_versions WHERE secret_name=? AND version_id=?",
                (name, version_id),
            )
        else:
            # find version with requested stage
            rows = self.storage.query(
                "SELECT * FROM sm_versions WHERE secret_name=? ORDER BY created_at DESC",
                (name,),
            )
            row = None
            for r in rows:
                stages = json.loads(r["stages_json"] or "[]")
                if version_stage in stages:
                    row = r
                    break
        if not row:
            raise NotFound(f"no secret version found for {name} (stage={version_stage})")
        import base64
        d = dict(row)
        binary_b64 = d.pop("secret_binary_b64", None)
        d["secret_binary"] = base64.b64decode(binary_b64).decode("latin-1") if binary_b64 else None
        d["version_stages"] = json.loads(d.pop("stages_json", "[]"))
        return d

    def list_secret_version_ids(self, name: str) -> list[dict]:
        self._require_secret(name)
        rows = self.storage.query(
            "SELECT version_id, stages_json, created_at FROM sm_versions WHERE secret_name=? ORDER BY created_at DESC",
            (name,),
        )
        result = []
        for r in rows:
            rd = dict(r)
            rd["version_stages"] = json.loads(rd.pop("stages_json", "[]"))
            result.append(rd)
        return result

    # ------------------------------------------------------------------
    # Rotation
    # ------------------------------------------------------------------

    def rotate_secret(self, name: str, rotation_lambda_arn: str,
                      rotation_rules: dict | None = None) -> dict[str, Any]:
        """Store rotation config (stub — does not invoke the Lambda)."""
        row = self._require_secret(name)
        self.storage.execute(
            "UPDATE sm_secrets SET rotation_enabled=1, rotation_lambda_arn=?, rotation_rules_json=? WHERE name=?",
            (rotation_lambda_arn, json.dumps(rotation_rules or {}), name),
        )
        return {"name": name, "arn": dict(row)["arn"], "rotation_enabled": True}

    # ------------------------------------------------------------------
    # Tags
    # ------------------------------------------------------------------

    def tag_resource(self, name: str, tags: list[dict]) -> None:
        self._require_secret(name)
        for tag in tags:
            self._upsert_tag(name, tag["Key"], tag["Value"])

    def untag_resource(self, name: str, tag_keys: list[str]) -> None:
        self._require_secret(name)
        for key in tag_keys:
            self.storage.execute(
                "DELETE FROM sm_tags WHERE secret_name=? AND tag_key=?", (name, key)
            )

    def list_tags_for_resource(self, name: str) -> list[dict]:
        self._require_secret(name)
        rows = self.storage.query(
            "SELECT tag_key, tag_value FROM sm_tags WHERE secret_name=?", (name,)
        )
        return [{"Key": r["tag_key"], "Value": r["tag_value"]} for r in rows]

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _require_secret(self, name: str) -> Any:
        row = self.storage.query_one("SELECT * FROM sm_secrets WHERE name=?", (name,))
        if not row:
            raise NotFound(f"no such secret: {name}")
        return row

    def _upsert_tag(self, name: str, key: str, value: str) -> None:
        existing = self.storage.query_one(
            "SELECT 1 FROM sm_tags WHERE secret_name=? AND tag_key=?", (name, key)
        )
        if existing:
            self.storage.execute(
                "UPDATE sm_tags SET tag_value=? WHERE secret_name=? AND tag_key=?",
                (value, name, key),
            )
        else:
            self.storage.execute(
                "INSERT INTO sm_tags(secret_name, tag_key, tag_value) VALUES (?,?,?)",
                (name, key, value),
            )
