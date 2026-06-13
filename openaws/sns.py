"""SNS-style pub/sub messaging.

Topics accept subscriptions from HTTP/SQS/Lambda endpoints and fan out
published messages to every subscriber. This implementation keeps
everything in-process / in-SQLite; the only "network" fan-out that
works out of the box is delivery into a co-located SQS queue or Lambda
function registered in the same App instance.

Protocol is JSON action on POST /sns.
"""

from __future__ import annotations

import json
import time
import uuid
from typing import Any

from .errors import Conflict, NotFound, ValidationError
from .storage import Storage


class SNSService:
    def __init__(self, storage: Storage):
        self.storage = storage
        # held at runtime for in-process SQS/Lambda fan-out
        self._sqs: Any = None
        self._lambdas: Any = None

    # ------------------------------------------------------------------
    # Topics
    # ------------------------------------------------------------------

    def create_topic(self, name: str) -> dict[str, Any]:
        if not name:
            raise ValidationError("topic name is required")
        if self.storage.query_one("SELECT name FROM sns_topics WHERE name=?", (name,)):
            raise Conflict(f"topic already exists: {name}")
        arn = f"arn:openaws:sns:local:000000000000:{name}"
        self.storage.execute(
            "INSERT INTO sns_topics(name, arn, created_at) VALUES (?,?,?)",
            (name, arn, time.time()),
        )
        return {"name": name, "arn": arn}

    def list_topics(self) -> list[dict[str, Any]]:
        rows = self.storage.query("SELECT name, arn FROM sns_topics ORDER BY name")
        return [dict(r) for r in rows]

    def delete_topic(self, name: str) -> None:
        self._require_topic(name)
        self.storage.execute("DELETE FROM sns_subscriptions WHERE topic=?", (name,))
        self.storage.execute("DELETE FROM sns_topics WHERE name=?", (name,))

    def _require_topic(self, name: str) -> dict[str, Any]:
        row = self.storage.query_one("SELECT * FROM sns_topics WHERE name=?", (name,))
        if not row:
            raise NotFound(f"no such topic: {name}")
        return dict(row)

    # ------------------------------------------------------------------
    # Subscriptions
    # ------------------------------------------------------------------

    def subscribe(self, topic: str, protocol: str, endpoint: str) -> dict[str, Any]:
        """Subscribe *endpoint* to *topic* using *protocol*.

        Supported protocols: ``sqs``, ``lambda``, ``log`` (records to
        sns_deliveries for inspection in tests).
        """
        if protocol not in ("sqs", "lambda", "log"):
            raise ValidationError(
                f"unsupported protocol {protocol!r}; choose sqs / lambda / log"
            )
        self._require_topic(topic)
        sub_id = uuid.uuid4().hex
        arn = f"arn:openaws:sns:local:000000000000:{topic}:{sub_id}"
        self.storage.execute(
            "INSERT INTO sns_subscriptions(id, arn, topic, protocol, endpoint, created_at)"
            " VALUES (?,?,?,?,?,?)",
            (sub_id, arn, topic, protocol, endpoint, time.time()),
        )
        return {"subscription_arn": arn, "topic": topic, "protocol": protocol,
                "endpoint": endpoint}

    def list_subscriptions(self, topic: str | None = None) -> list[dict[str, Any]]:
        if topic:
            self._require_topic(topic)
            rows = self.storage.query(
                "SELECT * FROM sns_subscriptions WHERE topic=? ORDER BY created_at",
                (topic,),
            )
        else:
            rows = self.storage.query(
                "SELECT * FROM sns_subscriptions ORDER BY created_at"
            )
        return [dict(r) for r in rows]

    def unsubscribe(self, subscription_arn: str) -> None:
        row = self.storage.query_one(
            "SELECT id FROM sns_subscriptions WHERE arn=?", (subscription_arn,)
        )
        if not row:
            raise NotFound(f"no such subscription: {subscription_arn}")
        self.storage.execute("DELETE FROM sns_subscriptions WHERE arn=?", (subscription_arn,))

    # ------------------------------------------------------------------
    # Publish (fan-out)
    # ------------------------------------------------------------------

    def publish(
        self,
        topic: str,
        message: str,
        subject: str | None = None,
        attributes: dict | None = None,
    ) -> dict[str, Any]:
        self._require_topic(topic)
        msg_id = uuid.uuid4().hex
        subs = self.storage.query(
            "SELECT * FROM sns_subscriptions WHERE topic=?", (topic,)
        )
        payload = json.dumps(
            {
                "Type": "Notification",
                "MessageId": msg_id,
                "TopicArn": f"arn:openaws:sns:local:000000000000:{topic}",
                "Subject": subject or "",
                "Message": message,
                "Timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "MessageAttributes": attributes or {},
            }
        )
        delivered = 0
        for sub in subs:
            proto = sub["protocol"]
            ep = sub["endpoint"]
            if proto == "sqs" and self._sqs:
                try:
                    self._sqs.send_message(ep, payload)
                    delivered += 1
                except Exception:  # noqa: BLE001 - best-effort delivery
                    pass
            elif proto == "lambda" and self._lambdas:
                try:
                    event = {"Records": [{"Sns": json.loads(payload)}]}
                    self._lambdas.invoke(ep, event)
                    delivered += 1
                except Exception:  # noqa: BLE001 - best-effort delivery
                    pass
            elif proto == "log":
                self.storage.execute(
                    "INSERT INTO sns_deliveries(msg_id, topic, endpoint, payload, ts)"
                    " VALUES (?,?,?,?,?)",
                    (msg_id, topic, ep, payload, time.time()),
                )
                delivered += 1
        return {"message_id": msg_id, "delivered": delivered}

    def get_deliveries(self, topic: str) -> list[dict[str, Any]]:
        """Return log-protocol deliveries (useful in tests)."""
        rows = self.storage.query(
            "SELECT * FROM sns_deliveries WHERE topic=? ORDER BY ts", (topic,)
        )
        return [dict(r) for r in rows]
