"""Cognito — User Pools.

Implements:
  - create_user_pool / describe_user_pool / delete_user_pool / list_user_pools
  - sign_up (create user with hashed password)
  - confirm_sign_up / admin_confirm_sign_up
  - initiate_auth (USER_PASSWORD_AUTH → returns JWT-style tokens)
  - get_user / admin_get_user / list_users
  - admin_create_user / admin_delete_user / admin_set_user_password
  - create_user_pool_client / describe_user_pool_client / list_user_pool_clients / delete_user_pool_client
  - global_sign_out (revoke tokens)
  - forgot_password / confirm_forgot_password (stub — stores reset code)
  - refresh_token  (issue new id/access tokens from a refresh token)

JWT-style tokens: openaws issues compact HMAC-SHA256 signed tokens with a
``header.payload.signature`` structure (base64url encoding, no padding).  The
issuer is ``openaws-local``; standard JWT libraries will NOT verify these, which
is fine for local development/testing.
"""

from __future__ import annotations

import base64
import hashlib
import hmac as _hmac
import json
import os
import time
import uuid
from typing import Any

from .errors import Conflict, NotFound, ValidationError
from .storage import Storage

_ARN_PREFIX = "arn:openaws:cognito-idp:local:000:userpool/"


def _arn(pool_id: str) -> str:
    return f"{_ARN_PREFIX}{pool_id}"


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _hash_password(password: str, salt: str) -> str:
    return hashlib.sha256((salt + password).encode()).hexdigest()


def _make_token(payload: dict, secret: str) -> str:
    header = _b64url(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    body = _b64url(json.dumps(payload).encode())
    sig = _b64url(
        _hmac.new(secret.encode(), f"{header}.{body}".encode(), hashlib.sha256).digest()
    )
    return f"{header}.{body}.{sig}"


def _verify_token(token: str, secret: str) -> dict | None:
    """Return payload dict if signature is valid, else None."""
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return None
        header, body, sig = parts
        expected_sig = _b64url(
            _hmac.new(secret.encode(), f"{header}.{body}".encode(), hashlib.sha256).digest()
        )
        if not _hmac.compare_digest(sig, expected_sig):
            return None
        # decode body (add back padding)
        padded = body + "=" * (-len(body) % 4)
        return json.loads(base64.urlsafe_b64decode(padded))
    except Exception:  # noqa: BLE001
        return None


class CognitoService:
    def __init__(self, storage: Storage):
        self.storage = storage

    # ------------------------------------------------------------------
    # User Pools
    # ------------------------------------------------------------------

    def create_user_pool(self, pool_name: str,
                          password_policy: dict | None = None,
                          auto_verified_attributes: list[str] | None = None,
                          username_attributes: list[str] | None = None) -> dict[str, Any]:
        if not pool_name:
            raise ValidationError("pool_name is required")
        pool_id = "local_" + uuid.uuid4().hex[:8]
        arn = _arn(pool_id)
        now = time.time()
        # Each pool has its own HMAC secret for token signing
        secret = os.urandom(32).hex()
        self.storage.execute(
            "INSERT INTO cognito_user_pools(pool_id, pool_name, arn, signing_secret,"
            " password_policy_json, auto_verified_attrs_json, username_attrs_json, created_at)"
            " VALUES (?,?,?,?,?,?,?,?)",
            (pool_id, pool_name, arn, secret,
             json.dumps(password_policy or {}),
             json.dumps(auto_verified_attributes or []),
             json.dumps(username_attributes or []),
             now),
        )
        return {"pool_id": pool_id, "pool_name": pool_name, "arn": arn}

    def describe_user_pool(self, pool_id: str) -> dict[str, Any]:
        row = self._require_pool(pool_id)
        d = dict(row)
        d.pop("signing_secret", None)
        d["password_policy"] = json.loads(d.pop("password_policy_json", "{}"))
        d["auto_verified_attributes"] = json.loads(d.pop("auto_verified_attrs_json", "[]"))
        d["username_attributes"] = json.loads(d.pop("username_attrs_json", "[]"))
        return d

    def delete_user_pool(self, pool_id: str) -> None:
        self._require_pool(pool_id)
        self.storage.execute("DELETE FROM cognito_user_pools WHERE pool_id=?", (pool_id,))
        self.storage.execute("DELETE FROM cognito_users WHERE pool_id=?", (pool_id,))
        self.storage.execute("DELETE FROM cognito_pool_clients WHERE pool_id=?", (pool_id,))
        self.storage.execute("DELETE FROM cognito_tokens WHERE pool_id=?", (pool_id,))

    def list_user_pools(self) -> list[dict]:
        rows = self.storage.query(
            "SELECT pool_id, pool_name, arn, created_at FROM cognito_user_pools ORDER BY pool_name"
        )
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Pool clients
    # ------------------------------------------------------------------

    def create_user_pool_client(self, pool_id: str, client_name: str,
                                  generate_secret: bool = False,
                                  explicit_auth_flows: list[str] | None = None) -> dict[str, Any]:
        self._require_pool(pool_id)
        client_id = uuid.uuid4().hex[:20]
        client_secret = uuid.uuid4().hex if generate_secret else None
        now = time.time()
        self.storage.execute(
            "INSERT INTO cognito_pool_clients(client_id, pool_id, client_name,"
            " client_secret, explicit_auth_flows_json, created_at) VALUES (?,?,?,?,?,?)",
            (client_id, pool_id, client_name, client_secret,
             json.dumps(explicit_auth_flows or []), now),
        )
        return {"client_id": client_id, "pool_id": pool_id, "client_name": client_name,
                "client_secret": client_secret}

    def describe_user_pool_client(self, pool_id: str, client_id: str) -> dict[str, Any]:
        row = self.storage.query_one(
            "SELECT * FROM cognito_pool_clients WHERE pool_id=? AND client_id=?",
            (pool_id, client_id),
        )
        if not row:
            raise NotFound(f"client not found: {client_id}")
        d = dict(row)
        d["explicit_auth_flows"] = json.loads(d.pop("explicit_auth_flows_json", "[]"))
        return d

    def list_user_pool_clients(self, pool_id: str) -> list[dict]:
        self._require_pool(pool_id)
        rows = self.storage.query(
            "SELECT client_id, pool_id, client_name FROM cognito_pool_clients WHERE pool_id=? ORDER BY client_name",
            (pool_id,),
        )
        return [dict(r) for r in rows]

    def delete_user_pool_client(self, pool_id: str, client_id: str) -> None:
        if not self.storage.query_one(
            "SELECT 1 FROM cognito_pool_clients WHERE pool_id=? AND client_id=?",
            (pool_id, client_id),
        ):
            raise NotFound(f"client not found: {client_id}")
        self.storage.execute(
            "DELETE FROM cognito_pool_clients WHERE pool_id=? AND client_id=?",
            (pool_id, client_id),
        )

    # ------------------------------------------------------------------
    # Users
    # ------------------------------------------------------------------

    def sign_up(self, pool_id: str, client_id: str, username: str,
                password: str, user_attributes: list[dict] | None = None) -> dict[str, Any]:
        self._require_pool(pool_id)
        if not self.storage.query_one(
            "SELECT 1 FROM cognito_pool_clients WHERE pool_id=? AND client_id=?",
            (pool_id, client_id),
        ):
            raise NotFound(f"client not found: {client_id}")
        if self.storage.query_one(
            "SELECT 1 FROM cognito_users WHERE pool_id=? AND username=?",
            (pool_id, username),
        ):
            raise Conflict(f"user already exists: {username}")
        salt = uuid.uuid4().hex
        hashed = _hash_password(password, salt)
        now = time.time()
        user_id = uuid.uuid4().hex
        attrs = json.dumps(user_attributes or [])
        self.storage.execute(
            "INSERT INTO cognito_users(user_id, pool_id, username, password_hash, password_salt,"
            " status, attributes_json, created_at) VALUES (?,?,?,?,?,?,?,?)",
            (user_id, pool_id, username, hashed, salt, "UNCONFIRMED", attrs, now),
        )
        confirmation_code = str(uuid.uuid4().int)[:6]
        self.storage.execute(
            "UPDATE cognito_users SET confirmation_code=? WHERE user_id=?",
            (confirmation_code, user_id),
        )
        return {
            "user_sub": user_id,
            "user_confirmed": False,
            "confirmation_code": confirmation_code,  # returned for test convenience
        }

    def confirm_sign_up(self, pool_id: str, client_id: str, username: str,
                         confirmation_code: str) -> dict[str, Any]:
        row = self.storage.query_one(
            "SELECT * FROM cognito_users WHERE pool_id=? AND username=?",
            (pool_id, username),
        )
        if not row:
            raise NotFound(f"user not found: {username}")
        if dict(row)["confirmation_code"] != confirmation_code:
            raise ValidationError("incorrect confirmation code")
        self.storage.execute(
            "UPDATE cognito_users SET status='CONFIRMED', confirmation_code=NULL WHERE user_id=?",
            (dict(row)["user_id"],),
        )
        return {"confirmed": True}

    def admin_confirm_sign_up(self, pool_id: str, username: str) -> dict[str, Any]:
        row = self.storage.query_one(
            "SELECT * FROM cognito_users WHERE pool_id=? AND username=?",
            (pool_id, username),
        )
        if not row:
            raise NotFound(f"user not found: {username}")
        self.storage.execute(
            "UPDATE cognito_users SET status='CONFIRMED', confirmation_code=NULL WHERE user_id=?",
            (dict(row)["user_id"],),
        )
        return {"confirmed": True}

    def admin_create_user(self, pool_id: str, username: str, temporary_password: str,
                           user_attributes: list[dict] | None = None) -> dict[str, Any]:
        self._require_pool(pool_id)
        if self.storage.query_one(
            "SELECT 1 FROM cognito_users WHERE pool_id=? AND username=?",
            (pool_id, username),
        ):
            raise Conflict(f"user already exists: {username}")
        salt = uuid.uuid4().hex
        hashed = _hash_password(temporary_password, salt)
        now = time.time()
        user_id = uuid.uuid4().hex
        attrs = json.dumps(user_attributes or [])
        self.storage.execute(
            "INSERT INTO cognito_users(user_id, pool_id, username, password_hash, password_salt,"
            " status, attributes_json, created_at) VALUES (?,?,?,?,?,?,?,?)",
            (user_id, pool_id, username, hashed, salt, "FORCE_CHANGE_PASSWORD", attrs, now),
        )
        return {"user_id": user_id, "username": username, "status": "FORCE_CHANGE_PASSWORD"}

    def admin_delete_user(self, pool_id: str, username: str) -> None:
        row = self.storage.query_one(
            "SELECT 1 FROM cognito_users WHERE pool_id=? AND username=?",
            (pool_id, username),
        )
        if not row:
            raise NotFound(f"user not found: {username}")
        self.storage.execute(
            "DELETE FROM cognito_users WHERE pool_id=? AND username=?",
            (pool_id, username),
        )

    def admin_set_user_password(self, pool_id: str, username: str,
                                  password: str, permanent: bool = True) -> None:
        row = self.storage.query_one(
            "SELECT * FROM cognito_users WHERE pool_id=? AND username=?",
            (pool_id, username),
        )
        if not row:
            raise NotFound(f"user not found: {username}")
        salt = uuid.uuid4().hex
        hashed = _hash_password(password, salt)
        new_status = "CONFIRMED" if permanent else "FORCE_CHANGE_PASSWORD"
        self.storage.execute(
            "UPDATE cognito_users SET password_hash=?, password_salt=?, status=? WHERE user_id=?",
            (hashed, salt, new_status, dict(row)["user_id"]),
        )

    def get_user(self, access_token: str) -> dict[str, Any]:
        payload = self._decode_token(access_token)
        pool_id = payload.get("pool_id")
        username = payload.get("sub")
        row = self.storage.query_one(
            "SELECT * FROM cognito_users WHERE pool_id=? AND username=?",
            (pool_id, username),
        )
        if not row:
            raise NotFound(f"user not found: {username}")
        return self._format_user(dict(row))

    def admin_get_user(self, pool_id: str, username: str) -> dict[str, Any]:
        row = self.storage.query_one(
            "SELECT * FROM cognito_users WHERE pool_id=? AND username=?",
            (pool_id, username),
        )
        if not row:
            raise NotFound(f"user not found: {username}")
        return self._format_user(dict(row))

    def list_users(self, pool_id: str, filter_str: str | None = None,
                    limit: int = 60) -> list[dict]:
        self._require_pool(pool_id)
        rows = self.storage.query(
            "SELECT * FROM cognito_users WHERE pool_id=? ORDER BY username LIMIT ?",
            (pool_id, limit),
        )
        result = [self._format_user(dict(r)) for r in rows]
        if filter_str:
            # simple "attribute = \"value\"" filter
            result = [u for u in result if filter_str.lower() in json.dumps(u).lower()]
        return result

    # ------------------------------------------------------------------
    # Auth
    # ------------------------------------------------------------------

    def initiate_auth(self, auth_flow: str, auth_parameters: dict,
                       client_id: str, pool_id: str | None = None) -> dict[str, Any]:
        if auth_flow not in ("USER_PASSWORD_AUTH", "REFRESH_TOKEN_AUTH",
                              "REFRESH_TOKEN", "USER_SRP_AUTH"):
            raise ValidationError(f"unsupported auth_flow: {auth_flow}")

        if auth_flow in ("REFRESH_TOKEN_AUTH", "REFRESH_TOKEN"):
            refresh_token = auth_parameters.get("REFRESH_TOKEN", "")
            return self._refresh_tokens(refresh_token, client_id, pool_id)

        username = auth_parameters.get("USERNAME") or auth_parameters.get("username")
        password = auth_parameters.get("PASSWORD") or auth_parameters.get("password")
        if not username or not password:
            raise ValidationError("USERNAME and PASSWORD are required for USER_PASSWORD_AUTH")

        # find the pool via client_id if pool_id not given
        if not pool_id:
            row = self.storage.query_one(
                "SELECT pool_id FROM cognito_pool_clients WHERE client_id=?", (client_id,)
            )
            if row:
                pool_id = row["pool_id"]
        if not pool_id:
            raise NotFound(f"client not found: {client_id}")

        user_row = self.storage.query_one(
            "SELECT * FROM cognito_users WHERE pool_id=? AND username=?",
            (pool_id, username),
        )
        if not user_row:
            raise NotFound(f"user not found: {username}")
        ud = dict(user_row)
        if ud["status"] == "UNCONFIRMED":
            raise ValidationError("user is not confirmed")
        expected = _hash_password(password, ud["password_salt"])
        if ud["password_hash"] != expected:
            raise ValidationError("incorrect username or password")

        pool_row = self._require_pool(pool_id)
        secret = dict(pool_row)["signing_secret"]
        return self._issue_tokens(pool_id, username, ud["user_id"], secret, client_id)

    def _issue_tokens(self, pool_id: str, username: str, user_id: str,
                       secret: str, client_id: str) -> dict[str, Any]:
        now = time.time()
        access_exp = now + 3600
        id_exp = now + 3600
        refresh_token = uuid.uuid4().hex + uuid.uuid4().hex

        access_payload = {
            "sub": username,
            "pool_id": pool_id,
            "client_id": client_id,
            "token_use": "access",
            "iss": "openaws-local",
            "iat": now,
            "exp": access_exp,
        }
        id_payload = {
            "sub": username,
            "pool_id": pool_id,
            "cognito:username": username,
            "token_use": "id",
            "iss": "openaws-local",
            "iat": now,
            "exp": id_exp,
        }
        access_token = _make_token(access_payload, secret)
        id_token = _make_token(id_payload, secret)

        self.storage.execute(
            "INSERT INTO cognito_tokens(refresh_token, pool_id, username, client_id, issued_at, expires_at)"
            " VALUES (?,?,?,?,?,?)",
            (refresh_token, pool_id, username, client_id, now, now + 30 * 86400),
        )
        return {
            "authentication_result": {
                "access_token": access_token,
                "id_token": id_token,
                "refresh_token": refresh_token,
                "token_type": "Bearer",
                "expires_in": 3600,
            }
        }

    def _refresh_tokens(self, refresh_token: str, client_id: str,
                         pool_id: str | None = None) -> dict[str, Any]:
        row = self.storage.query_one(
            "SELECT * FROM cognito_tokens WHERE refresh_token=?", (refresh_token,)
        )
        if not row:
            raise ValidationError("invalid refresh token")
        d = dict(row)
        if time.time() > d["expires_at"]:
            raise ValidationError("refresh token expired")
        pool_row = self._require_pool(d["pool_id"])
        secret = dict(pool_row)["signing_secret"]
        user_row = self.storage.query_one(
            "SELECT user_id FROM cognito_users WHERE pool_id=? AND username=?",
            (d["pool_id"], d["username"]),
        )
        user_id = dict(user_row)["user_id"] if user_row else d["username"]
        return self._issue_tokens(d["pool_id"], d["username"], user_id, secret, d["client_id"])

    def global_sign_out(self, access_token: str) -> dict[str, Any]:
        payload = self._decode_token(access_token)
        pool_id = payload.get("pool_id")
        username = payload.get("sub")
        self.storage.execute(
            "DELETE FROM cognito_tokens WHERE pool_id=? AND username=?",
            (pool_id, username),
        )
        return {"signed_out": True}

    # ------------------------------------------------------------------
    # Password reset (stub)
    # ------------------------------------------------------------------

    def forgot_password(self, pool_id: str, client_id: str,
                         username: str) -> dict[str, Any]:
        self._require_pool(pool_id)
        row = self.storage.query_one(
            "SELECT 1 FROM cognito_users WHERE pool_id=? AND username=?",
            (pool_id, username),
        )
        if not row:
            raise NotFound(f"user not found: {username}")
        reset_code = str(uuid.uuid4().int)[:6]
        self.storage.execute(
            "UPDATE cognito_users SET confirmation_code=? WHERE pool_id=? AND username=?",
            (reset_code, pool_id, username),
        )
        return {"code_delivery_details": {"delivery_medium": "EMAIL"},
                "reset_code": reset_code}  # returned for test convenience

    def confirm_forgot_password(self, pool_id: str, client_id: str, username: str,
                                  confirmation_code: str, new_password: str) -> dict[str, Any]:
        row = self.storage.query_one(
            "SELECT * FROM cognito_users WHERE pool_id=? AND username=?",
            (pool_id, username),
        )
        if not row:
            raise NotFound(f"user not found: {username}")
        if dict(row)["confirmation_code"] != confirmation_code:
            raise ValidationError("incorrect confirmation code")
        salt = uuid.uuid4().hex
        hashed = _hash_password(new_password, salt)
        self.storage.execute(
            "UPDATE cognito_users SET password_hash=?, password_salt=?, confirmation_code=NULL,"
            " status='CONFIRMED' WHERE user_id=?",
            (hashed, salt, dict(row)["user_id"]),
        )
        return {"password_reset": True}

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _require_pool(self, pool_id: str) -> Any:
        row = self.storage.query_one(
            "SELECT * FROM cognito_user_pools WHERE pool_id=?", (pool_id,)
        )
        if not row:
            raise NotFound(f"user pool not found: {pool_id}")
        return row

    def _format_user(self, d: dict) -> dict[str, Any]:
        attrs = json.loads(d.get("attributes_json", "[]"))
        return {
            "user_id": d["user_id"],
            "username": d["username"],
            "status": d["status"],
            "attributes": attrs,
            "created_at": d.get("created_at"),
        }

    def _decode_token(self, token: str) -> dict:
        """Decode token payload without verification (for pool lookup then verify)."""
        try:
            parts = token.split(".")
            padded = parts[1] + "=" * (-len(parts[1]) % 4)
            payload = json.loads(base64.urlsafe_b64decode(padded))
        except Exception as exc:
            raise ValidationError(f"invalid token: {exc}") from exc
        # verify signature using pool secret
        pool_id = payload.get("pool_id")
        if pool_id:
            row = self.storage.query_one(
                "SELECT signing_secret FROM cognito_user_pools WHERE pool_id=?", (pool_id,)
            )
            if row:
                verified = _verify_token(token, dict(row)["signing_secret"])
                if verified is None:
                    raise ValidationError("token signature invalid")
                return verified
        return payload
