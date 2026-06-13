"""Lambda-style function runner.

Register a Python function (either a live callable, or source code plus a
``handler`` name like ``"module.handler"``) and invoke it synchronously with
an event dict and a small context object. Also supports driving a function
from an SQS queue (poll-and-invoke) and from S3 object-created events,
mirroring the common AWS event-source patterns.

Functions registered from source run in a restricted namespace via ``exec``.
This is a LOCAL developer tool, not a security sandbox — only run code you
trust. Concurrency controls, layers, and timeouts are roadmap items.
"""

from __future__ import annotations

import json
import time
from typing import Any, Callable

from .errors import Conflict, NotFound, ValidationError
from .storage import Storage


class _Context:
    """A minimal Lambda-style context object."""

    def __init__(self, function_name: str):
        self.function_name = function_name
        self.aws_request_id = f"req-{int(time.time() * 1000)}"
        self.memory_limit_in_mb = 128

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

    def register_source(self, name: str, source: str, handler: str = "handler") -> dict[str, Any]:
        """Register a function from source code and a handler name."""
        if not name:
            raise ValidationError("function name is required")
        if name in self._callables or self._function_row(name):
            raise Conflict(f"function already exists: {name}")
        # validate that the source compiles and exposes the handler now
        self._build_from_source(source, handler)
        self.storage.execute(
            "INSERT INTO lambda_functions(name,source,handler,created_at) VALUES (?,?,?,?)",
            (name, source, handler, time.time()),
        )
        return {"name": name, "kind": "source", "handler": handler}

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

    def _resolve(self, name: str) -> Callable[[dict, Any], Any]:
        if name in self._callables:
            return self._callables[name]
        row = self._function_row(name)
        if not row:
            raise NotFound(f"no such function: {name}")
        return self._build_from_source(row["source"], row["handler"])

    def invoke(self, name: str, event: dict[str, Any] | None = None) -> Any:
        """Invoke a function synchronously and return its result."""
        fn = self._resolve(name)
        return fn(event or {}, _Context(name))

    # --- event sources -----------------------------------------------------
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
