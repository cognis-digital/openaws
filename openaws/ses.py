"""SES-style email capture service.

No real email is ever sent.  ``send_email`` stores the message in
SQLite; ``list_emails`` lets you retrieve them (useful in integration
tests to assert that an email was sent).

Identities (email addresses / domains) can be verified (stored) to
mirror the SES verified-sender workflow.

Protocol: JSON action on POST /ses.
"""

from __future__ import annotations

import json
import time
import uuid
from typing import Any

from .errors import Conflict, NotFound, ValidationError
from .storage import Storage


class SESService:
    def __init__(self, storage: Storage):
        self.storage = storage

    # ------------------------------------------------------------------
    # Identities
    # ------------------------------------------------------------------

    def verify_email_identity(self, email: str) -> dict[str, Any]:
        if not email or "@" not in email:
            raise ValidationError(f"invalid email address: {email!r}")
        existing = self.storage.query_one(
            "SELECT email FROM ses_identities WHERE email=?", (email,)
        )
        if existing:
            raise Conflict(f"identity already verified: {email}")
        self.storage.execute(
            "INSERT INTO ses_identities(email, verified_at) VALUES (?,?)",
            (email, time.time()),
        )
        return {"email": email, "status": "verified"}

    def list_identities(self) -> list[str]:
        rows = self.storage.query("SELECT email FROM ses_identities ORDER BY email")
        return [r["email"] for r in rows]

    def delete_identity(self, email: str) -> None:
        row = self.storage.query_one(
            "SELECT email FROM ses_identities WHERE email=?", (email,)
        )
        if not row:
            raise NotFound(f"no such identity: {email}")
        self.storage.execute("DELETE FROM ses_identities WHERE email=?", (email,))

    # ------------------------------------------------------------------
    # Send email
    # ------------------------------------------------------------------

    def send_email(
        self,
        source: str,
        to_addresses: list[str],
        subject: str,
        body_text: str | None = None,
        body_html: str | None = None,
        cc_addresses: list[str] | None = None,
        bcc_addresses: list[str] | None = None,
        reply_to: list[str] | None = None,
    ) -> dict[str, Any]:
        if not source:
            raise ValidationError("source (From) is required")
        if not to_addresses:
            raise ValidationError("to_addresses is required")
        if not subject:
            raise ValidationError("subject is required")
        if not body_text and not body_html:
            raise ValidationError("body_text or body_html is required")
        msg_id = f"{uuid.uuid4().hex}@openaws.local"
        payload = {
            "source": source,
            "to": to_addresses,
            "cc": cc_addresses or [],
            "bcc": bcc_addresses or [],
            "reply_to": reply_to or [],
            "subject": subject,
            "body_text": body_text or "",
            "body_html": body_html or "",
        }
        self.storage.execute(
            "INSERT INTO ses_emails(msg_id, payload_json, sent_at) VALUES (?,?,?)",
            (msg_id, json.dumps(payload), time.time()),
        )
        return {"message_id": msg_id}

    # ------------------------------------------------------------------
    # Retrieve captured emails (for testing)
    # ------------------------------------------------------------------

    def list_emails(
        self,
        to_address: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        rows = self.storage.query(
            "SELECT msg_id, payload_json, sent_at FROM ses_emails ORDER BY sent_at DESC"
        )
        results = []
        for row in rows:
            payload = json.loads(row["payload_json"])
            if to_address and to_address not in payload.get("to", []):
                continue
            payload["message_id"] = row["msg_id"]
            payload["sent_at"] = row["sent_at"]
            results.append(payload)
            if len(results) >= limit:
                break
        return results

    def get_email(self, msg_id: str) -> dict[str, Any]:
        row = self.storage.query_one(
            "SELECT * FROM ses_emails WHERE msg_id=?", (msg_id,)
        )
        if not row:
            raise NotFound(f"no such email: {msg_id}")
        out = json.loads(row["payload_json"])
        out["message_id"] = row["msg_id"]
        out["sent_at"] = row["sent_at"]
        return out

    def delete_emails(self) -> int:
        """Purge all captured emails (useful between tests)."""
        rows = self.storage.query("SELECT COUNT(*) AS c FROM ses_emails")
        count = rows[0]["c"] if rows else 0
        self.storage.execute("DELETE FROM ses_emails")
        return count
