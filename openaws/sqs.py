"""SQS-style message queue.

Real send/receive/delete semantics with a per-queue visibility timeout:
when a message is received it becomes invisible for ``visibility_timeout``
seconds; if it is not deleted in that window it becomes visible again and
can be redelivered. A receive returns a one-time receipt handle that must
be supplied to delete the message.

Compatible SUBSET: dead-letter queues, FIFO ordering guarantees, message
attributes, and long polling are roadmap items, not implemented.
"""

from __future__ import annotations

import time
import uuid
from typing import Any

from .errors import Conflict, NotFound, ValidationError
from .storage import Storage

DEFAULT_VISIBILITY_TIMEOUT = 30.0


class SQSService:
    def __init__(self, storage: Storage):
        self.storage = storage

    def create_queue(
        self, name: str, visibility_timeout: float = DEFAULT_VISIBILITY_TIMEOUT
    ) -> dict[str, Any]:
        if not name:
            raise ValidationError("queue name is required")
        if self.storage.query_one("SELECT name FROM sqs_queues WHERE name=?", (name,)):
            raise Conflict(f"queue already exists: {name}")
        self.storage.execute(
            "INSERT INTO sqs_queues(name,visibility_timeout,created_at) VALUES (?,?,?)",
            (name, float(visibility_timeout), time.time()),
        )
        return {"name": name, "visibility_timeout": float(visibility_timeout)}

    def list_queues(self) -> list[str]:
        rows = self.storage.query("SELECT name FROM sqs_queues ORDER BY name")
        return [r["name"] for r in rows]

    def delete_queue(self, name: str) -> None:
        self._require_queue(name)
        self.storage.execute("DELETE FROM sqs_messages WHERE queue=?", (name,))
        self.storage.execute("DELETE FROM sqs_queues WHERE name=?", (name,))

    def _require_queue(self, name: str):
        row = self.storage.query_one("SELECT * FROM sqs_queues WHERE name=?", (name,))
        if not row:
            raise NotFound(f"no such queue: {name}")
        return row

    def send_message(self, queue: str, body: str) -> dict[str, Any]:
        self._require_queue(queue)
        if not isinstance(body, str):
            raise ValidationError("message body must be a string")
        msg_id = uuid.uuid4().hex
        now = time.time()
        self.storage.execute(
            """INSERT INTO sqs_messages(id,queue,body,receipt_handle,visible_at,received_count,created_at)
               VALUES (?,?,?,?,?,?,?)""",
            (msg_id, queue, body, None, now, 0, now),
        )
        return {"message_id": msg_id}

    def receive_messages(self, queue: str, max_messages: int = 1) -> list[dict[str, Any]]:
        row = self._require_queue(queue)
        vt = row["visibility_timeout"]
        now = time.time()
        candidates = self.storage.query(
            "SELECT * FROM sqs_messages WHERE queue=? AND visible_at<=? "
            "ORDER BY created_at LIMIT ?",
            (queue, now, max(1, int(max_messages))),
        )
        out = []
        for m in candidates:
            handle = uuid.uuid4().hex
            self.storage.execute(
                "UPDATE sqs_messages SET receipt_handle=?, visible_at=?, received_count=? "
                "WHERE id=?",
                (handle, now + vt, m["received_count"] + 1, m["id"]),
            )
            out.append(
                {
                    "message_id": m["id"],
                    "body": m["body"],
                    "receipt_handle": handle,
                    "received_count": m["received_count"] + 1,
                }
            )
        return out

    def delete_message(self, queue: str, receipt_handle: str) -> bool:
        self._require_queue(queue)
        cur = self.storage.execute(
            "DELETE FROM sqs_messages WHERE queue=? AND receipt_handle=?",
            (queue, receipt_handle),
        )
        return cur.rowcount > 0

    def message_count(self, queue: str) -> int:
        self._require_queue(queue)
        return self.storage.query_one(
            "SELECT COUNT(*) AS c FROM sqs_messages WHERE queue=?", (queue,)
        )["c"]
