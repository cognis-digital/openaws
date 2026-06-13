"""IAM — Identity and Access Management.

Implements:
  - Users (create/get/delete/list, access keys)
  - Groups (create/delete/list, add/remove members)
  - Roles (create/delete/list, assume-role policy document)
  - Managed policies (create/delete/list/get)
  - Inline policies (put/get/delete for users, groups, roles)
  - Attach/detach managed policies to users/groups/roles
  - simulate_principal_policy — local Allow/Deny evaluator
"""

from __future__ import annotations

import json
import time
import uuid
from typing import Any

from .errors import Conflict, NotFound, ValidationError
from .storage import Storage

_ARN_PREFIX = "arn:openaws:iam::000:"


def _arn(resource_type: str, name: str) -> str:
    return f"{_ARN_PREFIX}{resource_type}/{name}"


class IAMService:
    def __init__(self, storage: Storage):
        self.storage = storage

    # ------------------------------------------------------------------
    # Users
    # ------------------------------------------------------------------

    def create_user(self, username: str, path: str = "/") -> dict[str, Any]:
        if not username:
            raise ValidationError("username is required")
        if self.storage.query_one("SELECT username FROM iam_users WHERE username=?", (username,)):
            raise Conflict(f"user already exists: {username}")
        now = time.time()
        arn = _arn("user", username)
        self.storage.execute(
            "INSERT INTO iam_users(username, path, arn, created_at) VALUES (?,?,?,?)",
            (username, path, arn, now),
        )
        return {"username": username, "path": path, "arn": arn, "created_at": now}

    def get_user(self, username: str) -> dict[str, Any]:
        row = self.storage.query_one("SELECT * FROM iam_users WHERE username=?", (username,))
        if not row:
            raise NotFound(f"no such user: {username}")
        return dict(row)

    def delete_user(self, username: str) -> None:
        self._require_user(username)
        self.storage.execute("DELETE FROM iam_users WHERE username=?", (username,))
        self.storage.execute("DELETE FROM iam_access_keys WHERE username=?", (username,))
        self.storage.execute("DELETE FROM iam_user_group_memberships WHERE username=?", (username,))
        self.storage.execute("DELETE FROM iam_inline_policies WHERE principal_type='user' AND principal_name=?", (username,))
        self.storage.execute("DELETE FROM iam_attachments WHERE principal_type='user' AND principal_name=?", (username,))

    def list_users(self, path_prefix: str = "/") -> list[dict]:
        rows = self.storage.query(
            "SELECT * FROM iam_users WHERE path LIKE ? ORDER BY username",
            (path_prefix + "%",),
        )
        return [dict(r) for r in rows]

    def _require_user(self, username: str) -> dict:
        row = self.storage.query_one("SELECT * FROM iam_users WHERE username=?", (username,))
        if not row:
            raise NotFound(f"no such user: {username}")
        return dict(row)

    # ------------------------------------------------------------------
    # Access keys
    # ------------------------------------------------------------------

    def create_access_key(self, username: str) -> dict[str, Any]:
        self._require_user(username)
        key_id = "AKIA" + uuid.uuid4().hex[:16].upper()
        secret = uuid.uuid4().hex + uuid.uuid4().hex
        now = time.time()
        self.storage.execute(
            "INSERT INTO iam_access_keys(key_id, secret_access_key, username, status, created_at)"
            " VALUES (?,?,?,?,?)",
            (key_id, secret, username, "Active", now),
        )
        return {"access_key_id": key_id, "secret_access_key": secret,
                "username": username, "status": "Active"}

    def list_access_keys(self, username: str) -> list[dict]:
        self._require_user(username)
        rows = self.storage.query(
            "SELECT key_id, username, status, created_at FROM iam_access_keys WHERE username=?",
            (username,),
        )
        return [dict(r) for r in rows]

    def delete_access_key(self, key_id: str) -> None:
        row = self.storage.query_one("SELECT key_id FROM iam_access_keys WHERE key_id=?", (key_id,))
        if not row:
            raise NotFound(f"no such access key: {key_id}")
        self.storage.execute("DELETE FROM iam_access_keys WHERE key_id=?", (key_id,))

    # ------------------------------------------------------------------
    # Groups
    # ------------------------------------------------------------------

    def create_group(self, group_name: str, path: str = "/") -> dict[str, Any]:
        if not group_name:
            raise ValidationError("group_name is required")
        if self.storage.query_one("SELECT group_name FROM iam_groups WHERE group_name=?", (group_name,)):
            raise Conflict(f"group already exists: {group_name}")
        now = time.time()
        arn = _arn("group", group_name)
        self.storage.execute(
            "INSERT INTO iam_groups(group_name, path, arn, created_at) VALUES (?,?,?,?)",
            (group_name, path, arn, now),
        )
        return {"group_name": group_name, "path": path, "arn": arn, "created_at": now}

    def delete_group(self, group_name: str) -> None:
        if not self.storage.query_one("SELECT group_name FROM iam_groups WHERE group_name=?", (group_name,)):
            raise NotFound(f"no such group: {group_name}")
        self.storage.execute("DELETE FROM iam_groups WHERE group_name=?", (group_name,))
        self.storage.execute("DELETE FROM iam_user_group_memberships WHERE group_name=?", (group_name,))
        self.storage.execute("DELETE FROM iam_inline_policies WHERE principal_type='group' AND principal_name=?", (group_name,))
        self.storage.execute("DELETE FROM iam_attachments WHERE principal_type='group' AND principal_name=?", (group_name,))

    def list_groups(self) -> list[dict]:
        rows = self.storage.query("SELECT * FROM iam_groups ORDER BY group_name")
        return [dict(r) for r in rows]

    def add_user_to_group(self, username: str, group_name: str) -> None:
        self._require_user(username)
        if not self.storage.query_one("SELECT group_name FROM iam_groups WHERE group_name=?", (group_name,)):
            raise NotFound(f"no such group: {group_name}")
        if self.storage.query_one(
            "SELECT 1 FROM iam_user_group_memberships WHERE username=? AND group_name=?",
            (username, group_name),
        ):
            return  # idempotent
        self.storage.execute(
            "INSERT INTO iam_user_group_memberships(username, group_name) VALUES (?,?)",
            (username, group_name),
        )

    def remove_user_from_group(self, username: str, group_name: str) -> None:
        self.storage.execute(
            "DELETE FROM iam_user_group_memberships WHERE username=? AND group_name=?",
            (username, group_name),
        )

    def list_groups_for_user(self, username: str) -> list[str]:
        self._require_user(username)
        rows = self.storage.query(
            "SELECT group_name FROM iam_user_group_memberships WHERE username=?",
            (username,),
        )
        return [r["group_name"] for r in rows]

    # ------------------------------------------------------------------
    # Roles
    # ------------------------------------------------------------------

    def create_role(self, role_name: str, assume_role_policy: dict,
                    description: str = "", path: str = "/") -> dict[str, Any]:
        if not role_name:
            raise ValidationError("role_name is required")
        if self.storage.query_one("SELECT role_name FROM iam_roles WHERE role_name=?", (role_name,)):
            raise Conflict(f"role already exists: {role_name}")
        now = time.time()
        arn = _arn("role", role_name)
        self.storage.execute(
            "INSERT INTO iam_roles(role_name, path, arn, assume_role_policy_json, description, created_at)"
            " VALUES (?,?,?,?,?,?)",
            (role_name, path, arn, json.dumps(assume_role_policy), description, now),
        )
        return {"role_name": role_name, "arn": arn, "path": path, "description": description}

    def get_role(self, role_name: str) -> dict[str, Any]:
        row = self.storage.query_one("SELECT * FROM iam_roles WHERE role_name=?", (role_name,))
        if not row:
            raise NotFound(f"no such role: {role_name}")
        d = dict(row)
        d["assume_role_policy"] = json.loads(d.pop("assume_role_policy_json", "{}"))
        return d

    def delete_role(self, role_name: str) -> None:
        if not self.storage.query_one("SELECT role_name FROM iam_roles WHERE role_name=?", (role_name,)):
            raise NotFound(f"no such role: {role_name}")
        self.storage.execute("DELETE FROM iam_roles WHERE role_name=?", (role_name,))
        self.storage.execute("DELETE FROM iam_inline_policies WHERE principal_type='role' AND principal_name=?", (role_name,))
        self.storage.execute("DELETE FROM iam_attachments WHERE principal_type='role' AND principal_name=?", (role_name,))

    def list_roles(self) -> list[dict]:
        rows = self.storage.query("SELECT role_name, path, arn, description, created_at FROM iam_roles ORDER BY role_name")
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Managed policies
    # ------------------------------------------------------------------

    def create_policy(self, policy_name: str, policy_document: dict,
                      description: str = "", path: str = "/") -> dict[str, Any]:
        if not policy_name:
            raise ValidationError("policy_name is required")
        if self.storage.query_one("SELECT policy_name FROM iam_policies WHERE policy_name=?", (policy_name,)):
            raise Conflict(f"policy already exists: {policy_name}")
        arn = _arn("policy", policy_name)
        now = time.time()
        self.storage.execute(
            "INSERT INTO iam_policies(policy_name, arn, path, description, document_json, created_at)"
            " VALUES (?,?,?,?,?,?)",
            (policy_name, arn, path, description, json.dumps(policy_document), now),
        )
        return {"policy_name": policy_name, "arn": arn, "path": path, "description": description}

    def get_policy(self, policy_name: str) -> dict[str, Any]:
        row = self.storage.query_one("SELECT * FROM iam_policies WHERE policy_name=?", (policy_name,))
        if not row:
            raise NotFound(f"no such policy: {policy_name}")
        d = dict(row)
        d["document"] = json.loads(d.pop("document_json", "{}"))
        return d

    def delete_policy(self, policy_name: str) -> None:
        if not self.storage.query_one("SELECT policy_name FROM iam_policies WHERE policy_name=?", (policy_name,)):
            raise NotFound(f"no such policy: {policy_name}")
        self.storage.execute("DELETE FROM iam_policies WHERE policy_name=?", (policy_name,))
        self.storage.execute("DELETE FROM iam_attachments WHERE policy_name=?", (policy_name,))

    def list_policies(self, scope: str = "Local") -> list[dict]:
        rows = self.storage.query(
            "SELECT policy_name, arn, path, description, created_at FROM iam_policies ORDER BY policy_name"
        )
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Inline policies
    # ------------------------------------------------------------------

    def put_inline_policy(self, principal_type: str, principal_name: str,
                          policy_name: str, policy_document: dict) -> None:
        """Upsert an inline policy on a user / group / role."""
        if principal_type not in ("user", "group", "role"):
            raise ValidationError("principal_type must be user, group, or role")
        existing = self.storage.query_one(
            "SELECT 1 FROM iam_inline_policies WHERE principal_type=? AND principal_name=? AND policy_name=?",
            (principal_type, principal_name, policy_name),
        )
        if existing:
            self.storage.execute(
                "UPDATE iam_inline_policies SET document_json=? WHERE principal_type=? AND principal_name=? AND policy_name=?",
                (json.dumps(policy_document), principal_type, principal_name, policy_name),
            )
        else:
            self.storage.execute(
                "INSERT INTO iam_inline_policies(principal_type, principal_name, policy_name, document_json)"
                " VALUES (?,?,?,?)",
                (principal_type, principal_name, policy_name, json.dumps(policy_document)),
            )

    def get_inline_policy(self, principal_type: str, principal_name: str,
                          policy_name: str) -> dict[str, Any]:
        row = self.storage.query_one(
            "SELECT * FROM iam_inline_policies WHERE principal_type=? AND principal_name=? AND policy_name=?",
            (principal_type, principal_name, policy_name),
        )
        if not row:
            raise NotFound(f"no such inline policy: {policy_name}")
        d = dict(row)
        d["document"] = json.loads(d.pop("document_json", "{}"))
        return d

    def delete_inline_policy(self, principal_type: str, principal_name: str,
                              policy_name: str) -> None:
        self.storage.execute(
            "DELETE FROM iam_inline_policies WHERE principal_type=? AND principal_name=? AND policy_name=?",
            (principal_type, principal_name, policy_name),
        )

    def list_inline_policies(self, principal_type: str, principal_name: str) -> list[str]:
        rows = self.storage.query(
            "SELECT policy_name FROM iam_inline_policies WHERE principal_type=? AND principal_name=? ORDER BY policy_name",
            (principal_type, principal_name),
        )
        return [r["policy_name"] for r in rows]

    # ------------------------------------------------------------------
    # Attach / detach managed policies
    # ------------------------------------------------------------------

    def attach_policy(self, principal_type: str, principal_name: str, policy_name: str) -> None:
        if principal_type not in ("user", "group", "role"):
            raise ValidationError("principal_type must be user, group, or role")
        if not self.storage.query_one("SELECT policy_name FROM iam_policies WHERE policy_name=?", (policy_name,)):
            raise NotFound(f"no such policy: {policy_name}")
        existing = self.storage.query_one(
            "SELECT 1 FROM iam_attachments WHERE principal_type=? AND principal_name=? AND policy_name=?",
            (principal_type, principal_name, policy_name),
        )
        if existing:
            return  # idempotent
        self.storage.execute(
            "INSERT INTO iam_attachments(principal_type, principal_name, policy_name) VALUES (?,?,?)",
            (principal_type, principal_name, policy_name),
        )

    def detach_policy(self, principal_type: str, principal_name: str, policy_name: str) -> None:
        self.storage.execute(
            "DELETE FROM iam_attachments WHERE principal_type=? AND principal_name=? AND policy_name=?",
            (principal_type, principal_name, policy_name),
        )

    def list_attached_policies(self, principal_type: str, principal_name: str) -> list[str]:
        rows = self.storage.query(
            "SELECT policy_name FROM iam_attachments WHERE principal_type=? AND principal_name=? ORDER BY policy_name",
            (principal_type, principal_name),
        )
        return [r["policy_name"] for r in rows]

    # ------------------------------------------------------------------
    # simulate_principal_policy
    # ------------------------------------------------------------------

    def simulate_principal_policy(self, principal_type: str, principal_name: str,
                                   action_names: list[str],
                                   resource_arns: list[str] | None = None) -> list[dict]:
        """
        Evaluate Allow/Deny for each (action, resource) pair against all
        inline + attached managed policies for the principal.  Returns a list
        of decision dicts.
        """
        resource_arns = resource_arns or ["*"]
        policies = self._collect_policies(principal_type, principal_name)
        results = []
        for action in action_names:
            for resource in resource_arns:
                decision = self._evaluate(action, resource, policies)
                results.append({
                    "eval_action_name": action,
                    "eval_resource_name": resource,
                    "eval_decision": decision,
                })
        return results

    def _collect_policies(self, principal_type: str, principal_name: str) -> list[dict]:
        docs = []
        # inline
        rows = self.storage.query(
            "SELECT document_json FROM iam_inline_policies WHERE principal_type=? AND principal_name=?",
            (principal_type, principal_name),
        )
        for r in rows:
            try:
                docs.append(json.loads(r["document_json"]))
            except Exception:  # noqa: BLE001
                pass
        # attached managed
        att_rows = self.storage.query(
            "SELECT policy_name FROM iam_attachments WHERE principal_type=? AND principal_name=?",
            (principal_type, principal_name),
        )
        for ar in att_rows:
            pr = self.storage.query_one(
                "SELECT document_json FROM iam_policies WHERE policy_name=?",
                (ar["policy_name"],),
            )
            if pr:
                try:
                    docs.append(json.loads(pr["document_json"]))
                except Exception:  # noqa: BLE001
                    pass
        return docs

    def _evaluate(self, action: str, resource: str, policies: list[dict]) -> str:
        """Returns 'allowed', 'explicitDeny', or 'implicitDeny'."""
        allowed = False
        for policy in policies:
            for stmt in policy.get("Statement", []):
                effect = stmt.get("Effect", "Deny")
                actions = stmt.get("Action", [])
                resources = stmt.get("Resource", [])
                if isinstance(actions, str):
                    actions = [actions]
                if isinstance(resources, str):
                    resources = [resources]
                action_match = any(
                    self._glob_match(a, action) for a in actions
                )
                resource_match = any(
                    self._glob_match(r, resource) for r in resources
                )
                if action_match and resource_match:
                    if effect == "Deny":
                        return "explicitDeny"
                    if effect == "Allow":
                        allowed = True
        return "allowed" if allowed else "implicitDeny"

    @staticmethod
    def _glob_match(pattern: str, value: str) -> bool:
        """Simple wildcard matching: * matches anything, ? matches one char."""
        import re as _re
        regex = "^" + _re.escape(pattern).replace(r"\*", ".*").replace(r"\?", ".") + "$"
        return bool(_re.match(regex, value, _re.IGNORECASE))
