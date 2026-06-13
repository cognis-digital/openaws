"""EventBridge-style event bus, rules, and targets.

Event buses accept events; rules match against event patterns
(subset of the real EB pattern matching: source, detail-type, and
detail field equality); matched rules fan out to Lambda or SQS targets
registered in the same App.

Protocol: JSON action on POST /eventbridge.
"""

from __future__ import annotations

import json
import re
import time
import uuid
from typing import Any

from .errors import Conflict, NotFound, ValidationError
from .storage import Storage


# ---------------------------------------------------------------------------
# Pattern matching
# ---------------------------------------------------------------------------

def _match_pattern(pattern: dict, event: dict) -> bool:
    """Return True if *event* matches *pattern*.

    Pattern format (subset):
        {
          "source": ["myapp.service"],
          "detail-type": ["UserCreated"],
          "detail": {
              "status": ["active", "pending"],
              "count": [{"numeric": [">", 5]}]
          }
        }

    Each list value is OR-ed.  Nested detail keys are matched recursively.
    Numeric conditions: ``{"numeric": [op, val]}`` where op is
    ``<``, ``<=``, ``=``, ``>=``, ``>``.
    Prefix match: ``{"prefix": "foo"}`` on string fields.
    Anything-but: ``{"anything-but": ["x", "y"]}``.
    """
    for key, pattern_val in pattern.items():
        event_val = event.get(key)
        if isinstance(pattern_val, dict):
            # nested detail match
            if not isinstance(event_val, dict):
                return False
            if not _match_pattern(pattern_val, event_val):
                return False
        elif isinstance(pattern_val, list):
            if not _match_value(pattern_val, event_val):
                return False
        else:
            return False
    return True


def _match_value(pattern_list: list, event_val: Any) -> bool:
    """Return True if *event_val* matches any element in *pattern_list*."""
    for item in pattern_list:
        if isinstance(item, dict):
            if "prefix" in item:
                if isinstance(event_val, str) and event_val.startswith(item["prefix"]):
                    return True
            elif "anything-but" in item:
                if event_val not in item["anything-but"]:
                    return True
            elif "numeric" in item:
                ops = item["numeric"]
                # ops is [op, val] or [op, val, op, val]
                if _numeric_match(ops, event_val):
                    return True
            elif "exists" in item:
                if item["exists"] is True and event_val is not None:
                    return True
                if item["exists"] is False and event_val is None:
                    return True
        else:
            if event_val == item:
                return True
    return False


def _numeric_match(ops: list, val: Any) -> bool:
    """Evaluate a numeric condition list against *val*."""
    if not isinstance(val, (int, float)):
        return False
    # ops may be [op, num] or [op, num, op, num]
    it = iter(ops)
    try:
        while True:
            op = next(it)
            num = next(it)
            if op == "<" and not (val < num):
                return False
            elif op == "<=" and not (val <= num):
                return False
            elif op == "=" and not (val == num):
                return False
            elif op == ">=" and not (val >= num):
                return False
            elif op == ">" and not (val > num):
                return False
    except StopIteration:
        return True
    return True


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------

class EventBridgeService:
    def __init__(self, storage: Storage):
        self.storage = storage
        self._sqs: Any = None
        self._lambdas: Any = None

    # ------------------------------------------------------------------
    # Buses
    # ------------------------------------------------------------------

    def create_event_bus(self, name: str) -> dict[str, Any]:
        if not name:
            raise ValidationError("bus name is required")
        if name == "default":
            raise ValidationError("'default' bus is created automatically; use put_event directly")
        if self.storage.query_one("SELECT name FROM eb_buses WHERE name=?", (name,)):
            raise Conflict(f"event bus already exists: {name}")
        arn = f"arn:openaws:events:local:000000000000:event-bus/{name}"
        self.storage.execute(
            "INSERT INTO eb_buses(name, arn, created_at) VALUES (?,?,?)",
            (name, arn, time.time()),
        )
        return {"name": name, "arn": arn}

    def list_event_buses(self) -> list[dict[str, Any]]:
        rows = self.storage.query("SELECT name, arn FROM eb_buses ORDER BY name")
        return [dict(r) for r in rows]

    def delete_event_bus(self, name: str) -> None:
        if name == "default":
            raise ValidationError("cannot delete the default event bus")
        self._require_bus(name)
        self.storage.execute("DELETE FROM eb_rules WHERE bus=?", (name,))
        self.storage.execute("DELETE FROM eb_buses WHERE name=?", (name,))

    def _require_bus(self, name: str) -> dict:
        row = self.storage.query_one("SELECT * FROM eb_buses WHERE name=?", (name,))
        if not row:
            raise NotFound(f"no such event bus: {name}")
        return dict(row)

    # ------------------------------------------------------------------
    # Rules
    # ------------------------------------------------------------------

    def put_rule(
        self,
        name: str,
        bus: str = "default",
        event_pattern: dict | None = None,
        schedule_expression: str | None = None,
        state: str = "ENABLED",
    ) -> dict[str, Any]:
        if not name:
            raise ValidationError("rule name is required")
        if not event_pattern and not schedule_expression:
            raise ValidationError("event_pattern or schedule_expression is required")
        # ensure bus exists (default is always available)
        if bus != "default":
            self._require_bus(bus)
        arn = f"arn:openaws:events:local:000000000000:rule/{name}"
        pattern_json = json.dumps(event_pattern) if event_pattern else None
        existing = self.storage.query_one(
            "SELECT name FROM eb_rules WHERE name=? AND bus=?", (name, bus)
        )
        if existing:
            self.storage.execute(
                "UPDATE eb_rules SET pattern_json=?, schedule=?, state=? WHERE name=? AND bus=?",
                (pattern_json, schedule_expression, state, name, bus),
            )
        else:
            self.storage.execute(
                "INSERT INTO eb_rules(name, bus, arn, pattern_json, schedule, state, created_at)"
                " VALUES (?,?,?,?,?,?,?)",
                (name, bus, arn, pattern_json, schedule_expression, state, time.time()),
            )
        return {"name": name, "bus": bus, "arn": arn, "state": state}

    def list_rules(self, bus: str = "default") -> list[dict[str, Any]]:
        rows = self.storage.query(
            "SELECT * FROM eb_rules WHERE bus=? ORDER BY name", (bus,)
        )
        result = []
        for r in rows:
            d = dict(r)
            if d.get("pattern_json"):
                d["event_pattern"] = json.loads(d["pattern_json"])
            d.pop("pattern_json", None)
            result.append(d)
        return result

    def delete_rule(self, name: str, bus: str = "default") -> None:
        row = self.storage.query_one(
            "SELECT name FROM eb_rules WHERE name=? AND bus=?", (name, bus)
        )
        if not row:
            raise NotFound(f"no such rule: {name} on bus {bus}")
        self.storage.execute("DELETE FROM eb_targets WHERE rule=? AND bus=?", (name, bus))
        self.storage.execute("DELETE FROM eb_rules WHERE name=? AND bus=?", (name, bus))

    # ------------------------------------------------------------------
    # Targets
    # ------------------------------------------------------------------

    def put_targets(self, rule: str, bus: str = "default", targets: list[dict] | None = None) -> dict[str, Any]:
        """Attach targets to a rule.  Each target: {id, arn, type} where
        type is "lambda" or "sqs".
        """
        row = self.storage.query_one(
            "SELECT name FROM eb_rules WHERE name=? AND bus=?", (rule, bus)
        )
        if not row:
            raise NotFound(f"no such rule: {rule}")
        failed = []
        for t in (targets or []):
            tid = t.get("id") or uuid.uuid4().hex
            ttype = t.get("type", "")
            tarn = t.get("arn", "")
            if not ttype or not tarn:
                failed.append({"id": tid, "error": "type and arn are required"})
                continue
            existing = self.storage.query_one(
                "SELECT id FROM eb_targets WHERE rule=? AND bus=? AND id=?",
                (rule, bus, tid),
            )
            if existing:
                self.storage.execute(
                    "UPDATE eb_targets SET type=?, arn=? WHERE rule=? AND bus=? AND id=?",
                    (ttype, tarn, rule, bus, tid),
                )
            else:
                self.storage.execute(
                    "INSERT INTO eb_targets(rule, bus, id, type, arn, created_at)"
                    " VALUES (?,?,?,?,?,?)",
                    (rule, bus, tid, ttype, tarn, time.time()),
                )
        return {"failed": failed}

    def list_targets(self, rule: str, bus: str = "default") -> list[dict[str, Any]]:
        rows = self.storage.query(
            "SELECT id, type, arn FROM eb_targets WHERE rule=? AND bus=? ORDER BY id",
            (rule, bus),
        )
        return [dict(r) for r in rows]

    def remove_targets(self, rule: str, bus: str = "default", ids: list[str] | None = None) -> None:
        for tid in (ids or []):
            self.storage.execute(
                "DELETE FROM eb_targets WHERE rule=? AND bus=? AND id=?",
                (rule, bus, tid),
            )

    # ------------------------------------------------------------------
    # Events
    # ------------------------------------------------------------------

    def put_events(self, events: list[dict]) -> dict[str, Any]:
        """Route events to matching rule targets.

        Each event dict: {bus, source, detail_type, detail (dict)}
        Returns {failed_entry_count, entries: [{event_id, ...}]}.
        """
        results = []
        failed = 0
        for ev in events:
            bus = ev.get("bus", "default")
            event_id = uuid.uuid4().hex
            structured = {
                "id": event_id,
                "source": ev.get("source", ""),
                "detail-type": ev.get("detail_type", ""),
                "detail": ev.get("detail", {}),
                "time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "bus": bus,
            }
            rules = self.storage.query(
                "SELECT * FROM eb_rules WHERE bus=? AND state='ENABLED'", (bus,)
            )
            for rule in rules:
                if rule["pattern_json"]:
                    pattern = json.loads(rule["pattern_json"])
                    if not _match_pattern(pattern, structured):
                        continue
                targets = self.storage.query(
                    "SELECT * FROM eb_targets WHERE rule=? AND bus=?",
                    (rule["name"], bus),
                )
                for t in targets:
                    self._invoke_target(t, structured)
            results.append({"event_id": event_id})
        return {"failed_entry_count": failed, "entries": results}

    def _invoke_target(self, target: Any, event: dict) -> None:
        ttype = target["type"]
        tarn = target["arn"]
        # ARN encodes the resource name as the last segment
        resource = tarn.split(":")[-1].split("/")[-1]
        if ttype == "lambda" and self._lambdas:
            try:
                self._lambdas.invoke(resource, event)
            except Exception:  # noqa: BLE001
                pass
        elif ttype == "sqs" and self._sqs:
            try:
                self._sqs.send_message(resource, json.dumps(event))
            except Exception:  # noqa: BLE001
                pass
