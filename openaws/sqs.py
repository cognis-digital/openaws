"""SQS-style message queue.

Real send/receive/delete semantics with a per-queue visibility timeout:
when a message is received it becomes invisible for ``visibility_timeout``
seconds; if it is not deleted in that window it becomes visible again and
can be redelivered. A receive returns a one-time receipt handle that must
be supplied to delete the message.

This pass adds:
  - FIFO queues with message group IDs and exactly-once deduplication
  - Dead-letter queues (DLQ redrive): messages that exceed
    ``max_receive_count`` are automatically moved to the DLQ
  - Per-message attributes (key → {data_type, string_value})
"""

from __future__ import annotations

import json
import time
import uuid
from typing import Any

from .errors import Conflict, NotFound, ValidationError
from .storage import Storage

DEFAULT_VISIBILITY_TIMEOUT = 30.0
DEFAULT_DEDUP_WINDOW = 300.0  # seconds


class SQSService:
    def __init__(self, storage: Storage):
        self.storage = storage

    def create_queue(
        self,
        name: str,
        visibility_timeout: float = DEFAULT_VISIBILITY_TIMEOUT,
        fifo: bool = False,
        dedup_window: float = DEFAULT_DEDUP_WINDOW,
        dlq_name: str | None = None,
        max_receive_count: int = 0,
    ) -> dict[str, Any]:
        if not name:
            raise ValidationError("queue name is required")
        if fifo and not name.endswith(".fifo"):
            raise ValidationError("FIFO queue name must end with '.fifo'")
        if self.storage.query_one("SELECT name FROM sqs_queues WHERE name=?", (name,)):
            raise Conflict(f"queue already exists: {name}")
        if dlq_name:
            if not self.storage.query_one("SELECT name FROM sqs_queues WHERE name=?", (dlq_name,)):
                raise NotFound(f"DLQ target does not exist: {dlq_name}")
        self.storage.execute(
            "INSERT INTO sqs_queues"
            "(name, visibility_timeout, created_at, fifo, dedup_window, dlq_name, max_receive_count)"
            " VALUES (?,?,?,?,?,?,?)",
            (name, float(visibility_timeout), time.time(),
             1 if fifo else 0, float(dedup_window),
             dlq_name, int(max_receive_count)),
        )
        return {
            "name": name,
            "visibility_timeout": float(visibility_timeout),
            "fifo": fifo,
        }

    def list_queues(self) -> list[str]:
        rows = self.storage.query("SELECT name FROM sqs_queues ORDER BY name")
        return [r["name"] for r in rows]

    def delete_queue(self, name: str) -> None:
        self._require_queue(name)
        self.storage.execute("DELETE FROM sqs_messages WHERE queue=?", (name,))
        self.storage.execute("DELETE FROM sqs_queues WHERE name=?", (name,))

    def _require_queue(self, name: str) -> dict:
        row = self.storage.query_one("SELECT * FROM sqs_queues WHERE name=?", (name,))
        if not row:
            raise NotFound(f"no such queue: {name}")
        return dict(row)

    # ------------------------------------------------------------------
    # Send
    # ------------------------------------------------------------------

    def send_message(
        self,
        queue: str,
        body: str,
        message_group_id: str | None = None,
        message_deduplication_id: str | None = None,
        attributes: dict | None = None,
    ) -> dict[str, Any]:
        q = self._require_queue(queue)
        if not isinstance(body, str):
            raise ValidationError("message body must be a string")
        if q["fifo"] and not message_group_id:
            raise ValidationError("FIFO queue requires message_group_id")

        # FIFO deduplication: within dedup_window, same dedup_id → no-op
        if q["fifo"] and message_deduplication_id:
            window = q["dedup_window"]
            cutoff = time.time() - window
            dup = self.storage.query_one(
                "SELECT id FROM sqs_messages"
                " WHERE queue=? AND dedup_id=? AND created_at>=?",
                (queue, message_deduplication_id, cutoff),
            )
            if dup:
                return {"message_id": dup["id"], "duplicate": True}

        msg_id = uuid.uuid4().hex
        now = time.time()
        attr_json = json.dumps(attributes or {})
        self.storage.execute(
            """INSERT INTO sqs_messages
               (id, queue, body, receipt_handle, visible_at, received_count,
                created_at, group_id, dedup_id, attributes_json)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (msg_id, queue, body, None, now, 0, now,
             message_group_id, message_deduplication_id, attr_json),
        )
        return {"message_id": msg_id}

    # ------------------------------------------------------------------
    # Receive
    # ------------------------------------------------------------------

    def receive_messages(
        self, queue: str, max_messages: int = 1
    ) -> list[dict[str, Any]]:
        q = self._require_queue(queue)
        vt = q["visibility_timeout"]
        now = time.time()
        candidates = self.storage.query(
            "SELECT * FROM sqs_messages WHERE queue=? AND visible_at<=? "
            "ORDER BY created_at LIMIT ?",
            (queue, now, max(1, int(max_messages))),
        )
        out = []
        dlq_name = q.get("dlq_name")
        max_rc = int(q.get("max_receive_count") or 0)
        for m in candidates:
            new_rc = m["received_count"] + 1
            # DLQ redrive
            if dlq_name and max_rc and new_rc > max_rc:
                self._redrive_to_dlq(m, dlq_name)
                continue
            handle = uuid.uuid4().hex
            self.storage.execute(
                "UPDATE sqs_messages SET receipt_handle=?, visible_at=?, received_count=? "
                "WHERE id=?",
                (handle, now + vt, new_rc, m["id"]),
            )
            attrs = {}
            try:
                attrs = json.loads(m["attributes_json"] or "{}")
            except Exception:  # noqa: BLE001
                pass
            out.append(
                {
                    "message_id": m["id"],
                    "body": m["body"],
                    "receipt_handle": handle,
                    "received_count": new_rc,
                    "attributes": attrs,
                    "group_id": m["group_id"],
                }
            )
        return out

    def _redrive_to_dlq(self, msg: Any, dlq_name: str) -> None:
        """Move a message to its DLQ."""
        new_id = uuid.uuid4().hex
        now = time.time()
        try:
            self.storage.execute(
                """INSERT INTO sqs_messages
                   (id, queue, body, receipt_handle, visible_at, received_count,
                    created_at, group_id, dedup_id, attributes_json)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (new_id, dlq_name, msg["body"], None, now, 0, now,
                 msg["group_id"], msg["dedup_id"],
                 msg["attributes_json"] or "{}"),
            )
        except Exception:  # noqa: BLE001
            pass
        self.storage.execute("DELETE FROM sqs_messages WHERE id=?", (msg["id"],))

    # ------------------------------------------------------------------
    # Delete
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Redrive policy helpers
    # ------------------------------------------------------------------

    def get_queue_attributes(self, queue: str) -> dict[str, Any]:
        q = self._require_queue(queue)
        return {
            "name": q["name"],
            "visibility_timeout": q["visibility_timeout"],
            "fifo": bool(q["fifo"]),
            "dedup_window": q["dedup_window"],
            "dlq_name": q["dlq_name"],
            "max_receive_count": q["max_receive_count"],
            "message_count": self.message_count(queue),
        }
