"""STS — Security Token Service.

Implements:
  - AssumeRole       — issue temporary credentials for a role
  - GetCallerIdentity — return account/caller info for current credentials
  - GetSessionToken  — issue temporary session credentials
"""

from __future__ import annotations

import time
import uuid
from typing import Any

from .errors import NotFound, ValidationError
from .storage import Storage

_ACCOUNT_ID = "000000000000"
_REGION = "local"


class STSService:
    def __init__(self, storage: Storage):
        self.storage = storage

    # ------------------------------------------------------------------
    # AssumeRole
    # ------------------------------------------------------------------

    def assume_role(
        self,
        role_arn: str,
        role_session_name: str,
        duration_seconds: int = 3600,
        external_id: str | None = None,
    ) -> dict[str, Any]:
        if not role_arn:
            raise ValidationError("role_arn is required")
        if not role_session_name:
            raise ValidationError("role_session_name is required")
        # look up role by ARN or name
        row = self.storage.query_one(
            "SELECT * FROM iam_roles WHERE arn=? OR role_name=?",
            (role_arn, role_arn),
        )
        if not row:
            raise NotFound(f"no such role: {role_arn}")

        now = time.time()
        expiry = now + max(900, min(int(duration_seconds), 43200))
        access_key_id = "ASIA" + uuid.uuid4().hex[:16].upper()
        secret_key = uuid.uuid4().hex + uuid.uuid4().hex
        session_token = uuid.uuid4().hex + uuid.uuid4().hex + uuid.uuid4().hex

        self.storage.execute(
            "INSERT INTO sts_sessions(session_id, access_key_id, secret_key, session_token,"
            " assumed_role_arn, session_name, issued_at, expires_at)"
            " VALUES (?,?,?,?,?,?,?,?)",
            (uuid.uuid4().hex, access_key_id, secret_key, session_token,
             role_arn, role_session_name, now, expiry),
        )
        return {
            "credentials": {
                "access_key_id": access_key_id,
                "secret_access_key": secret_key,
                "session_token": session_token,
                "expiration": expiry,
            },
            "assumed_role_user": {
                "assumed_role_id": f"{dict(row)['role_name']}:{role_session_name}",
                "arn": f"arn:openaws:sts::{_ACCOUNT_ID}:assumed-role/{dict(row)['role_name']}/{role_session_name}",
            },
        }

    # ------------------------------------------------------------------
    # GetSessionToken
    # ------------------------------------------------------------------

    def get_session_token(self, duration_seconds: int = 43200) -> dict[str, Any]:
        now = time.time()
        expiry = now + max(900, min(int(duration_seconds), 129600))
        access_key_id = "ASIA" + uuid.uuid4().hex[:16].upper()
        secret_key = uuid.uuid4().hex + uuid.uuid4().hex
        session_token = uuid.uuid4().hex + uuid.uuid4().hex + uuid.uuid4().hex
        self.storage.execute(
            "INSERT INTO sts_sessions(session_id, access_key_id, secret_key, session_token,"
            " assumed_role_arn, session_name, issued_at, expires_at)"
            " VALUES (?,?,?,?,?,?,?,?)",
            (uuid.uuid4().hex, access_key_id, secret_key, session_token,
             None, "GetSessionToken", now, expiry),
        )
        return {
            "credentials": {
                "access_key_id": access_key_id,
                "secret_access_key": secret_key,
                "session_token": session_token,
                "expiration": expiry,
            }
        }

    # ------------------------------------------------------------------
    # GetCallerIdentity
    # ------------------------------------------------------------------

    def get_caller_identity(self, access_key_id: str | None = None) -> dict[str, Any]:
        """Return identity info.

        If ``access_key_id`` is provided and matches a session, return the
        assumed-role ARN.  Otherwise return a generic local identity.
        """
        if access_key_id:
            row = self.storage.query_one(
                "SELECT * FROM sts_sessions WHERE access_key_id=?",
                (access_key_id,),
            )
            if row:
                d = dict(row)
                arn = (
                    d["assumed_role_arn"]
                    if d.get("assumed_role_arn")
                    else f"arn:openaws:iam::{_ACCOUNT_ID}:root"
                )
                return {
                    "user_id": d["session_id"],
                    "account": _ACCOUNT_ID,
                    "arn": arn,
                }
            # check regular access keys
            key_row = self.storage.query_one(
                "SELECT * FROM iam_access_keys WHERE key_id=?",
                (access_key_id,),
            )
            if key_row:
                kd = dict(key_row)
                return {
                    "user_id": kd["key_id"],
                    "account": _ACCOUNT_ID,
                    "arn": f"arn:openaws:iam::{_ACCOUNT_ID}:user/{kd['username']}",
                }
        return {
            "user_id": "AIDAROOT",
            "account": _ACCOUNT_ID,
            "arn": f"arn:openaws:iam::{_ACCOUNT_ID}:root",
        }

    # ------------------------------------------------------------------
    # Session management
    # ------------------------------------------------------------------

    def list_sessions(self) -> list[dict]:
        rows = self.storage.query(
            "SELECT session_id, access_key_id, assumed_role_arn, session_name, issued_at, expires_at"
            " FROM sts_sessions ORDER BY issued_at"
        )
        return [dict(r) for r in rows]

    def revoke_session(self, access_key_id: str) -> None:
        self.storage.execute(
            "DELETE FROM sts_sessions WHERE access_key_id=?", (access_key_id,)
        )
