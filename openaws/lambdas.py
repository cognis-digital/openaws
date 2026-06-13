"""Lambda-style function runner.

Register a Python function (either a live callable, or source code plus a
``handler`` name like ``"module.handler"``) and invoke it synchronously with
an event dict and a small context object. Also supports driving a function
from an SQS queue (poll-and-invoke) and from S3 object-created events,
mirroring the common AWS event-source patterns.

This pass adds:
  - Environment variables per function (env_vars dict, injected into context)
  - Function versions (publish_version) and aliases (create/update/list alias)
  - Async invocation (invoke_async queues the event; process_async_queue runs them)
  - Layers metadata (add_layer_version / list_layer_versions / list_layers)

Functions registered from source run in a restricted namespace via ``exec``.
This is a LOCAL developer tool, not a security sandbox — only run code you
trust. Concurrency controls and timeouts are not enforced.
"""

from __future__ import annotations

import json
import time
import uuid
from typing import Any, Callable

from .errors import Conflict, NotFound, ValidationError
from .storage import Storage


class _Context:
    """A minimal Lambda-style context object."""

    def __init__(self, function_name: str, env: dict | None = None):
        self.function_name = function_name
        self.aws_request_id = f"req-{int(time.time() * 1000)}"
        self.memory_limit_in_mb = 128
        self.env = env or {}

    def get_remaining_time_in_millis(self) -> int:  # pragma: no cover - trivial
        return 3000


class LambdaService:
    def __init__(self, storage: Storage):
        self.storage = storage
        # live (non-persisted) callables registered in-process
        self._callables: dict[str, Callable[[dict, Any], Any]] = {}

    def register_callable(self, name: str, fn: Callable[[dict, Any], Any]) -> dict[str, Any]:
        """Register an in-process Python callable as a function."""
        if not name:
            raise ValidationError("function name is required")
        if not callable(fn):
            raise ValidationError("fn must be callable")
        if name in self._callables or self._function_row(name):
            raise Conflict(f"function already exists: {name}")
        self._callables[name] = fn
        return {"name": name, "kind": "callable"}

    def register_source(
        self,
        name: str,
        source: str,
        handler: str = "handler",
        env_vars: dict | None = None,
        description: str = "",
        timeout: int = 3,
    ) -> dict[str, Any]:
        """Register a function from source code and a handler name."""
        if not name:
            raise ValidationError("function name is required")
        if name in self._callables or self._function_row(name):
            raise Conflict(f"function already exists: {name}")
        # validate that the source compiles and exposes the handler now
        self._build_from_source(source, handler)
        env_json = json.dumps(env_vars or {})
        self.storage.execute(
            "INSERT INTO lambda_functions"
            "(name, source, handler, created_at, env_json, description, timeout)"
            " VALUES (?,?,?,?,?,?,?)",
            (name, source, handler, time.time(), env_json, description, timeout),
        )
        return {"name": name, "kind": "source", "handler": handler}

    def update_function_configuration(
        self,
        name: str,
        env_vars: dict | None = None,
        description: str | None = None,
        timeout: int | None = None,
    ) -> dict[str, Any]:
        row = self._function_row(name)
        if not row:
            raise NotFound(f"no such function: {name}")
        updates: list[str] = []
        params: list[Any] = []
        if env_vars is not None:
            updates.append("env_json=?")
            params.append(json.dumps(env_vars))
        if description is not None:
            updates.append("description=?")
            params.append(description)
        if timeout is not None:
            updates.append("timeout=?")
            params.append(int(timeout))
        if updates:
            params.append(name)
            self.storage.execute(
                f"UPDATE lambda_functions SET {', '.join(updates)} WHERE name=?",
                tuple(params),
            )
        return self.get_function(name)

    def get_function(self, name: str) -> dict[str, Any]:
        row = self._function_row(name)
        if not row and name not in self._callables:
            raise NotFound(f"no such function: {name}")
        if row:
            env = {}
            try:
                env = json.loads(row["env_json"] or "{}")
            except Exception:  # noqa: BLE001
                pass
            return {
                "name": row["name"],
                "handler": row["handler"],
                "description": row["description"] or "",
                "timeout": row["timeout"] or 3,
                "env_vars": env,
            }
        return {"name": name, "kind": "callable"}

    def list_functions(self) -> list[str]:
        rows = self.storage.query("SELECT name FROM lambda_functions ORDER BY name")
        names = {r["name"] for r in rows} | set(self._callables)
        return sorted(names)

    def delete_function(self, name: str) -> None:
        if name in self._callables:
            del self._callables[name]
            return
        if not self._function_row(name):
            raise NotFound(f"no such function: {name}")
        self.storage.execute("DELETE FROM lambda_functions WHERE name=?", (name,))
        self.storage.execute(
            "DELETE FROM lambda_versions WHERE function_name=?", (name,)
        )
        self.storage.execute(
            "DELETE FROM lambda_aliases WHERE function_name=?", (name,)
        )

    def _function_row(self, name: str):
        return self.storage.query_one(
            "SELECT * FROM lambda_functions WHERE name=?", (name,)
        )

    @staticmethod
    def _build_from_source(source: str, handler: str) -> Callable[[dict, Any], Any]:
        ns: dict[str, Any] = {}
        try:
            code = compile(source, "<openaws-lambda>", "exec")
        except SyntaxError as exc:  # pragma: no cover - defensive
            raise ValidationError(f"function source does not compile: {exc}") from exc
        exec(code, ns)  # noqa: S102 - intentional local dev runner
        fn_name = handler.split(".")[-1]
        fn = ns.get(fn_name)
        if not callable(fn):
            raise ValidationError(f"handler {handler!r} not found in source")
        return fn

    def _resolve(self, name: str) -> tuple[Callable[[dict, Any], Any], dict]:
        """Return (callable, env_vars) for *name*."""
        if name in self._callables:
            return self._callables[name], {}
        row = self._function_row(name)
        if not row:
            raise NotFound(f"no such function: {name}")
        env: dict = {}
        try:
            env = json.loads(row["env_json"] or "{}")
        except Exception:  # noqa: BLE001
            pass
        return self._build_from_source(row["source"], row["handler"]), env

    def invoke(self, name: str, event: dict[str, Any] | None = None) -> Any:
        """Invoke a function synchronously and return its result."""
        fn, env = self._resolve(name)
        return fn(event or {}, _Context(name, env))

    # ------------------------------------------------------------------
    # Versions
    # ------------------------------------------------------------------

    def publish_version(
        self, name: str, description: str = ""
    ) -> dict[str, Any]:
        """Snapshot the current function code as a numbered version."""
        row = self._function_row(name)
        if not row:
            raise NotFound(f"no such function: {name}")
        # version number = next integer
        existing = self.storage.query(
            "SELECT version FROM lambda_versions WHERE function_name=? ORDER BY version",
            (name,),
        )
        version = str(len(existing) + 1)
        self.storage.execute(
            "INSERT INTO lambda_versions"
            "(function_name, version, source, handler, env_json, description, created_at)"
            " VALUES (?,?,?,?,?,?,?)",
            (name, version, row["source"], row["handler"],
             row["env_json"] or "{}", description, time.time()),
        )
        return {"function_name": name, "version": version, "description": description}

    def list_versions(self, name: str) -> list[dict[str, Any]]:
        self._require_function(name)
        rows = self.storage.query(
            "SELECT version, description, created_at FROM lambda_versions"
            " WHERE function_name=? ORDER BY CAST(version AS INTEGER)",
            (name,),
        )
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Aliases
    # ------------------------------------------------------------------

    def create_alias(
        self,
        name: str,
        alias: str,
        version: str,
        description: str = "",
    ) -> dict[str, Any]:
        self._require_function(name)
        existing = self.storage.query_one(
            "SELECT alias FROM lambda_aliases WHERE function_name=? AND alias=?",
            (name, alias),
        )
        if existing:
            raise Conflict(f"alias already exists: {alias} on {name}")
        self.storage.execute(
            "INSERT INTO lambda_aliases"
            "(function_name, alias, version, description, created_at)"
            " VALUES (?,?,?,?,?)",
            (name, alias, version, description, time.time()),
        )
        return {"function_name": name, "alias": alias, "version": version}

    def update_alias(
        self,
        name: str,
        alias: str,
        version: str,
        description: str | None = None,
    ) -> dict[str, Any]:
        row = self.storage.query_one(
            "SELECT alias FROM lambda_aliases WHERE function_name=? AND alias=?",
            (name, alias),
        )
        if not row:
            raise NotFound(f"no such alias: {alias} on {name}")
        updates = ["version=?"]
        params: list[Any] = [version]
        if description is not None:
            updates.append("description=?")
            params.append(description)
        params.extend([name, alias])
        self.storage.execute(
            f"UPDATE lambda_aliases SET {', '.join(updates)}"
            " WHERE function_name=? AND alias=?",
            tuple(params),
        )
        return {"function_name": name, "alias": alias, "version": version}

    def list_aliases(self, name: str) -> list[dict[str, Any]]:
        self._require_function(name)
        rows = self.storage.query(
            "SELECT alias, version, description FROM lambda_aliases"
            " WHERE function_name=? ORDER BY alias",
            (name,),
        )
        return [dict(r) for r in rows]

    def delete_alias(self, name: str, alias: str) -> None:
        row = self.storage.query_one(
            "SELECT alias FROM lambda_aliases WHERE function_name=? AND alias=?",
            (name, alias),
        )
        if not row:
            raise NotFound(f"no such alias: {alias} on {name}")
        self.storage.execute(
            "DELETE FROM lambda_aliases WHERE function_name=? AND alias=?",
            (name, alias),
        )

    def _require_function(self, name: str):
        if name not in self._callables and not self._function_row(name):
            raise NotFound(f"no such function: {name}")

    # ------------------------------------------------------------------
    # Async invocation
    # ------------------------------------------------------------------

    def invoke_async(
        self, name: str, event: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Queue the event for asynchronous execution.  Returns immediately."""
        self._require_function(name)
        inv_id = uuid.uuid4().hex
        self.storage.execute(
            "INSERT INTO lambda_async_queue(id, function_name, event_json, status, created_at)"
            " VALUES (?,?,?,?,?)",
            (inv_id, name, json.dumps(event or {}), "QUEUED", time.time()),
        )
        return {"invocation_id": inv_id, "status": "QUEUED"}

    def process_async_queue(self, function_name: str | None = None) -> list[dict[str, Any]]:
        """Process queued async invocations.  Call from tests or a worker loop."""
        if function_name:
            rows = self.storage.query(
                "SELECT * FROM lambda_async_queue WHERE function_name=? AND status='QUEUED'"
                " ORDER BY created_at",
                (function_name,),
            )
        else:
            rows = self.storage.query(
                "SELECT * FROM lambda_async_queue WHERE status='QUEUED' ORDER BY created_at"
            )
        results = []
        for row in rows:
            event = json.loads(row["event_json"])
            try:
                out = self.invoke(row["function_name"], event)
                status = "SUCCEEDED"
            except Exception as exc:  # noqa: BLE001
                out = str(exc)
                status = "FAILED"
            self.storage.execute(
                "UPDATE lambda_async_queue SET status=? WHERE id=?",
                (status, row["id"]),
            )
            results.append({
                "invocation_id": row["id"],
                "function_name": row["function_name"],
                "status": status,
                "result": out,
            })
        return results

    def list_async_invocations(
        self, function_name: str | None = None
    ) -> list[dict[str, Any]]:
        if function_name:
            rows = self.storage.query(
                "SELECT id, function_name, status, created_at FROM lambda_async_queue"
                " WHERE function_name=? ORDER BY created_at DESC",
                (function_name,),
            )
        else:
            rows = self.storage.query(
                "SELECT id, function_name, status, created_at FROM lambda_async_queue"
                " ORDER BY created_at DESC"
            )
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Layers metadata
    # ------------------------------------------------------------------

    def add_layer_version(
        self,
        name: str,
        description: str = "",
        compatible_runtimes: list[str] | None = None,
    ) -> dict[str, Any]:
        if not name:
            raise ValidationError("layer name is required")
        existing = self.storage.query(
            "SELECT version FROM lambda_layers WHERE name=? ORDER BY version DESC",
            (name,),
        )
        version = (existing[0]["version"] + 1) if existing else 1
        runtimes_json = json.dumps(compatible_runtimes or [])
        self.storage.execute(
            "INSERT INTO lambda_layers(name, version, description, compatible_runtimes_json, created_at)"
            " VALUES (?,?,?,?,?)",
            (name, version, description, runtimes_json, time.time()),
        )
        return {
            "name": name,
            "version": version,
            "description": description,
            "compatible_runtimes": compatible_runtimes or [],
        }

    def list_layer_versions(self, name: str) -> list[dict[str, Any]]:
        rows = self.storage.query(
            "SELECT version, description, compatible_runtimes_json, created_at"
            " FROM lambda_layers WHERE name=? ORDER BY version",
            (name,),
        )
        result = []
        for r in rows:
            d = dict(r)
            d["compatible_runtimes"] = json.loads(d.pop("compatible_runtimes_json", "[]"))
            result.append(d)
        return result

    def list_layers(self) -> list[dict[str, Any]]:
        rows = self.storage.query(
            "SELECT DISTINCT name FROM lambda_layers ORDER BY name"
        )
        result = []
        for r in rows:
            versions = self.list_layer_versions(r["name"])
            latest = versions[-1] if versions else {}
            result.append({"name": r["name"], "latest_version": latest})
        return result

    # ------------------------------------------------------------------
    # Event sources
    # ------------------------------------------------------------------

    def invoke_from_sqs(self, name: str, sqs, queue: str, max_messages: int = 10) -> list[Any]:
        """Poll an SQS queue and invoke the function once per message.

        Successfully processed messages are deleted from the queue, matching
        the AWS SQS event-source behaviour (delete on success).
        """
        messages = sqs.receive_messages(queue, max_messages=max_messages)
        if not messages:
            return []
        event = {
            "Records": [
                {
                    "messageId": m["message_id"],
                    "receiptHandle": m["receipt_handle"],
                    "body": m["body"],
                    "eventSource": "openaws:sqs",
                }
                for m in messages
            ]
        }
        result = self.invoke(name, event)
        for m in messages:
            sqs.delete_message(queue, m["receipt_handle"])
        return [result]

    def invoke_from_s3_put(self, name: str, bucket: str, key: str, size: int = 0) -> Any:
        """Invoke the function with an S3 ObjectCreated:Put-style event."""
        event = {
            "Records": [
                {
                    "eventSource": "openaws:s3",
                    "eventName": "ObjectCreated:Put",
                    "s3": {
                        "bucket": {"name": bucket},
                        "object": {"key": key, "size": size},
                    },
                }
            ]
        }
        return self.invoke(name, event)

    @staticmethod
    def as_json(value: Any) -> str:  # pragma: no cover - helper
        return json.dumps(value)
