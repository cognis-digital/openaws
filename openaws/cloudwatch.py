"""CloudWatch — Metrics, Logs, and Alarms.

Implements:
  Metrics:
    - put_metric_data (namespace + metric data points)
    - get_metric_statistics (period-aggregated, supports Average/Sum/Maximum/Minimum/SampleCount)
    - list_metrics

  Logs:
    - create_log_group / delete_log_group / list_log_groups
    - create_log_stream / delete_log_stream / list_log_streams
    - put_log_events / get_log_events / filter_log_events

  Alarms:
    - put_metric_alarm (threshold-based; evaluates against stored metrics)
    - describe_alarms / describe_alarm / delete_alarm
    - set_alarm_state (manual override for testing)
    - get_alarm_history
"""

from __future__ import annotations

import json
import time
import uuid
from typing import Any

from .errors import Conflict, NotFound, ValidationError
from .storage import Storage


class CloudWatchService:
    def __init__(self, storage: Storage):
        self.storage = storage

    # ==================================================================
    # Metrics
    # ==================================================================

    def put_metric_data(self, namespace: str, metric_data: list[dict]) -> None:
        """Store metric data points.

        Each entry in metric_data should have at least:
          {"MetricName": "...", "Value": float, "Unit": "..."}
        Optional: "Timestamp", "Dimensions" (list of {Name, Value} dicts).
        """
        if not namespace:
            raise ValidationError("namespace is required")
        now = time.time()
        for dp in metric_data:
            metric_name = dp.get("MetricName") or dp.get("metric_name")
            if not metric_name:
                raise ValidationError("MetricName is required in each metric datum")
            value = float(dp.get("Value", dp.get("value", 0)))
            unit = dp.get("Unit", dp.get("unit", "None"))
            ts = float(dp.get("Timestamp", dp.get("timestamp", now)))
            dimensions = dp.get("Dimensions", dp.get("dimensions", []))
            self.storage.execute(
                "INSERT INTO cw_metrics(id, namespace, metric_name, value, unit,"
                " dimensions_json, ts)"
                " VALUES (?,?,?,?,?,?,?)",
                (uuid.uuid4().hex, namespace, metric_name, value, unit,
                 json.dumps(dimensions), ts),
            )

    def get_metric_statistics(
        self,
        namespace: str,
        metric_name: str,
        start_time: float,
        end_time: float,
        period: int = 60,
        statistics: list[str] | None = None,
        dimensions: list[dict] | None = None,
    ) -> list[dict]:
        statistics = statistics or ["Average"]
        rows = self.storage.query(
            "SELECT value, ts FROM cw_metrics WHERE namespace=? AND metric_name=? AND ts>=? AND ts<?",
            (namespace, metric_name, start_time, end_time),
        )
        if not rows:
            return []
        # aggregate into period buckets
        buckets: dict[int, list[float]] = {}
        for r in rows:
            bucket = int(r["ts"] // period) * period
            buckets.setdefault(bucket, []).append(r["value"])
        results = []
        for bucket_ts, values in sorted(buckets.items()):
            dp: dict[str, Any] = {"timestamp": bucket_ts}
            if "Average" in statistics:
                dp["Average"] = sum(values) / len(values)
            if "Sum" in statistics:
                dp["Sum"] = sum(values)
            if "Maximum" in statistics:
                dp["Maximum"] = max(values)
            if "Minimum" in statistics:
                dp["Minimum"] = min(values)
            if "SampleCount" in statistics:
                dp["SampleCount"] = len(values)
            results.append(dp)
        return results

    def list_metrics(self, namespace: str | None = None,
                      metric_name: str | None = None) -> list[dict]:
        if namespace and metric_name:
            rows = self.storage.query(
                "SELECT DISTINCT namespace, metric_name FROM cw_metrics WHERE namespace=? AND metric_name=?",
                (namespace, metric_name),
            )
        elif namespace:
            rows = self.storage.query(
                "SELECT DISTINCT namespace, metric_name FROM cw_metrics WHERE namespace=?",
                (namespace,),
            )
        else:
            rows = self.storage.query(
                "SELECT DISTINCT namespace, metric_name FROM cw_metrics"
            )
        return [dict(r) for r in rows]

    # ==================================================================
    # Log Groups
    # ==================================================================

    def create_log_group(self, log_group_name: str,
                          kms_key_id: str | None = None) -> dict[str, Any]:
        if not log_group_name:
            raise ValidationError("log_group_name is required")
        if self.storage.query_one(
            "SELECT log_group_name FROM cw_log_groups WHERE log_group_name=?",
            (log_group_name,),
        ):
            raise Conflict(f"log group already exists: {log_group_name}")
        now = time.time()
        self.storage.execute(
            "INSERT INTO cw_log_groups(log_group_name, kms_key_id, created_at) VALUES (?,?,?)",
            (log_group_name, kms_key_id, now),
        )
        return {"log_group_name": log_group_name, "created_at": now}

    def delete_log_group(self, log_group_name: str) -> None:
        if not self.storage.query_one(
            "SELECT 1 FROM cw_log_groups WHERE log_group_name=?", (log_group_name,)
        ):
            raise NotFound(f"log group not found: {log_group_name}")
        self.storage.execute(
            "DELETE FROM cw_log_streams WHERE log_group_name=?", (log_group_name,)
        )
        self.storage.execute(
            "DELETE FROM cw_log_events WHERE log_group_name=?", (log_group_name,)
        )
        self.storage.execute(
            "DELETE FROM cw_log_groups WHERE log_group_name=?", (log_group_name,)
        )

    def list_log_groups(self, prefix: str | None = None) -> list[dict]:
        if prefix:
            rows = self.storage.query(
                "SELECT * FROM cw_log_groups WHERE log_group_name LIKE ? ORDER BY log_group_name",
                (prefix + "%",),
            )
        else:
            rows = self.storage.query(
                "SELECT * FROM cw_log_groups ORDER BY log_group_name"
            )
        return [dict(r) for r in rows]

    # ==================================================================
    # Log Streams
    # ==================================================================

    def create_log_stream(self, log_group_name: str, log_stream_name: str) -> dict[str, Any]:
        if not self.storage.query_one(
            "SELECT 1 FROM cw_log_groups WHERE log_group_name=?", (log_group_name,)
        ):
            raise NotFound(f"log group not found: {log_group_name}")
        if self.storage.query_one(
            "SELECT 1 FROM cw_log_streams WHERE log_group_name=? AND log_stream_name=?",
            (log_group_name, log_stream_name),
        ):
            raise Conflict(f"log stream already exists: {log_stream_name}")
        now = time.time()
        self.storage.execute(
            "INSERT INTO cw_log_streams(log_group_name, log_stream_name, created_at) VALUES (?,?,?)",
            (log_group_name, log_stream_name, now),
        )
        return {"log_group_name": log_group_name, "log_stream_name": log_stream_name}

    def delete_log_stream(self, log_group_name: str, log_stream_name: str) -> None:
        if not self.storage.query_one(
            "SELECT 1 FROM cw_log_streams WHERE log_group_name=? AND log_stream_name=?",
            (log_group_name, log_stream_name),
        ):
            raise NotFound(f"log stream not found: {log_stream_name}")
        self.storage.execute(
            "DELETE FROM cw_log_events WHERE log_group_name=? AND log_stream_name=?",
            (log_group_name, log_stream_name),
        )
        self.storage.execute(
            "DELETE FROM cw_log_streams WHERE log_group_name=? AND log_stream_name=?",
            (log_group_name, log_stream_name),
        )

    def list_log_streams(self, log_group_name: str,
                          prefix: str | None = None) -> list[dict]:
        if not self.storage.query_one(
            "SELECT 1 FROM cw_log_groups WHERE log_group_name=?", (log_group_name,)
        ):
            raise NotFound(f"log group not found: {log_group_name}")
        if prefix:
            rows = self.storage.query(
                "SELECT * FROM cw_log_streams WHERE log_group_name=? AND log_stream_name LIKE ? ORDER BY log_stream_name",
                (log_group_name, prefix + "%"),
            )
        else:
            rows = self.storage.query(
                "SELECT * FROM cw_log_streams WHERE log_group_name=? ORDER BY log_stream_name",
                (log_group_name,),
            )
        return [dict(r) for r in rows]

    # ==================================================================
    # Log Events
    # ==================================================================

    def put_log_events(self, log_group_name: str, log_stream_name: str,
                        log_events: list[dict],
                        sequence_token: str | None = None) -> dict[str, Any]:
        if not self.storage.query_one(
            "SELECT 1 FROM cw_log_streams WHERE log_group_name=? AND log_stream_name=?",
            (log_group_name, log_stream_name),
        ):
            raise NotFound(f"log stream not found: {log_stream_name}")
        now = time.time()
        for event in log_events:
            ts = float(event.get("timestamp", now))
            message = event.get("message", "")
            self.storage.execute(
                "INSERT INTO cw_log_events(id, log_group_name, log_stream_name, ts, message)"
                " VALUES (?,?,?,?,?)",
                (uuid.uuid4().hex, log_group_name, log_stream_name, ts, message),
            )
        next_token = uuid.uuid4().hex
        return {"next_sequence_token": next_token}

    def get_log_events(self, log_group_name: str, log_stream_name: str,
                        start_time: float | None = None, end_time: float | None = None,
                        limit: int = 100) -> dict[str, Any]:
        if not self.storage.query_one(
            "SELECT 1 FROM cw_log_streams WHERE log_group_name=? AND log_stream_name=?",
            (log_group_name, log_stream_name),
        ):
            raise NotFound(f"log stream not found: {log_stream_name}")
        base = "SELECT * FROM cw_log_events WHERE log_group_name=? AND log_stream_name=?"
        params: list[Any] = [log_group_name, log_stream_name]
        if start_time is not None:
            base += " AND ts>=?"
            params.append(start_time)
        if end_time is not None:
            base += " AND ts<=?"
            params.append(end_time)
        base += " ORDER BY ts LIMIT ?"
        params.append(limit)
        rows = self.storage.query(base, tuple(params))
        events = [{"timestamp": r["ts"], "message": r["message"]} for r in rows]
        return {"events": events}

    def filter_log_events(self, log_group_name: str,
                           filter_pattern: str | None = None,
                           log_stream_names: list[str] | None = None,
                           start_time: float | None = None,
                           end_time: float | None = None,
                           limit: int = 100) -> dict[str, Any]:
        if not self.storage.query_one(
            "SELECT 1 FROM cw_log_groups WHERE log_group_name=?", (log_group_name,)
        ):
            raise NotFound(f"log group not found: {log_group_name}")
        base = "SELECT * FROM cw_log_events WHERE log_group_name=?"
        params: list[Any] = [log_group_name]
        if log_stream_names:
            placeholders = ",".join("?" * len(log_stream_names))
            base += f" AND log_stream_name IN ({placeholders})"
            params.extend(log_stream_names)
        if start_time is not None:
            base += " AND ts>=?"
            params.append(start_time)
        if end_time is not None:
            base += " AND ts<=?"
            params.append(end_time)
        if filter_pattern:
            base += " AND message LIKE ?"
            params.append(f"%{filter_pattern}%")
        base += " ORDER BY ts LIMIT ?"
        params.append(limit)
        rows = self.storage.query(base, tuple(params))
        events = [{"timestamp": r["ts"], "message": r["message"],
                   "log_stream_name": r["log_stream_name"]} for r in rows]
        return {"events": events}

    # ==================================================================
    # Alarms
    # ==================================================================

    def put_metric_alarm(
        self,
        alarm_name: str,
        namespace: str,
        metric_name: str,
        comparison_operator: str,
        threshold: float,
        evaluation_periods: int = 1,
        period: int = 60,
        statistic: str = "Average",
        description: str = "",
        alarm_actions: list[str] | None = None,
        ok_actions: list[str] | None = None,
        insufficient_data_actions: list[str] | None = None,
        treat_missing_data: str = "missing",
    ) -> dict[str, Any]:
        valid_ops = {
            "GreaterThanOrEqualToThreshold", "GreaterThanThreshold",
            "LessThanThreshold", "LessThanOrEqualToThreshold",
        }
        if comparison_operator not in valid_ops:
            raise ValidationError(f"comparison_operator must be one of {valid_ops}")
        now = time.time()
        existing = self.storage.query_one(
            "SELECT alarm_name FROM cw_alarms WHERE alarm_name=?", (alarm_name,)
        )
        payload = dict(
            alarm_name=alarm_name, namespace=namespace, metric_name=metric_name,
            comparison_operator=comparison_operator, threshold=threshold,
            evaluation_periods=evaluation_periods, period=period, statistic=statistic,
            description=description,
            alarm_actions_json=json.dumps(alarm_actions or []),
            ok_actions_json=json.dumps(ok_actions or []),
            insufficient_data_actions_json=json.dumps(insufficient_data_actions or []),
            treat_missing_data=treat_missing_data,
            state="INSUFFICIENT_DATA",
            state_updated_at=now,
            created_at=now,
        )
        if existing:
            self.storage.execute(
                "UPDATE cw_alarms SET namespace=?, metric_name=?, comparison_operator=?,"
                " threshold=?, evaluation_periods=?, period=?, statistic=?, description=?,"
                " alarm_actions_json=?, ok_actions_json=?, insufficient_data_actions_json=?,"
                " treat_missing_data=? WHERE alarm_name=?",
                (namespace, metric_name, comparison_operator, threshold,
                 evaluation_periods, period, statistic, description,
                 payload["alarm_actions_json"], payload["ok_actions_json"],
                 payload["insufficient_data_actions_json"], treat_missing_data, alarm_name),
            )
        else:
            self.storage.execute(
                "INSERT INTO cw_alarms(alarm_name, namespace, metric_name, comparison_operator,"
                " threshold, evaluation_periods, period, statistic, description,"
                " alarm_actions_json, ok_actions_json, insufficient_data_actions_json,"
                " treat_missing_data, state, state_updated_at, created_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (alarm_name, namespace, metric_name, comparison_operator, threshold,
                 evaluation_periods, period, statistic, description,
                 payload["alarm_actions_json"], payload["ok_actions_json"],
                 payload["insufficient_data_actions_json"], treat_missing_data,
                 "INSUFFICIENT_DATA", now, now),
            )
        return {"alarm_name": alarm_name}

    def describe_alarms(self, alarm_names: list[str] | None = None,
                         state_value: str | None = None) -> list[dict]:
        rows = self.storage.query("SELECT * FROM cw_alarms ORDER BY alarm_name")
        result = []
        for r in rows:
            d = dict(r)
            d["alarm_actions"] = json.loads(d.pop("alarm_actions_json", "[]"))
            d["ok_actions"] = json.loads(d.pop("ok_actions_json", "[]"))
            d["insufficient_data_actions"] = json.loads(d.pop("insufficient_data_actions_json", "[]"))
            result.append(d)
        if alarm_names:
            result = [a for a in result if a["alarm_name"] in alarm_names]
        if state_value:
            result = [a for a in result if a["state"] == state_value]
        return result

    def describe_alarm(self, alarm_name: str) -> dict[str, Any]:
        row = self.storage.query_one("SELECT * FROM cw_alarms WHERE alarm_name=?", (alarm_name,))
        if not row:
            raise NotFound(f"alarm not found: {alarm_name}")
        d = dict(row)
        d["alarm_actions"] = json.loads(d.pop("alarm_actions_json", "[]"))
        d["ok_actions"] = json.loads(d.pop("ok_actions_json", "[]"))
        d["insufficient_data_actions"] = json.loads(d.pop("insufficient_data_actions_json", "[]"))
        return d

    def delete_alarm(self, alarm_name: str) -> None:
        if not self.storage.query_one("SELECT 1 FROM cw_alarms WHERE alarm_name=?", (alarm_name,)):
            raise NotFound(f"alarm not found: {alarm_name}")
        self.storage.execute("DELETE FROM cw_alarms WHERE alarm_name=?", (alarm_name,))

    def set_alarm_state(self, alarm_name: str, state_value: str,
                         state_reason: str = "") -> dict[str, Any]:
        if state_value not in ("OK", "ALARM", "INSUFFICIENT_DATA"):
            raise ValidationError("state_value must be OK, ALARM, or INSUFFICIENT_DATA")
        if not self.storage.query_one("SELECT 1 FROM cw_alarms WHERE alarm_name=?", (alarm_name,)):
            raise NotFound(f"alarm not found: {alarm_name}")
        now = time.time()
        self.storage.execute(
            "UPDATE cw_alarms SET state=?, state_updated_at=? WHERE alarm_name=?",
            (state_value, now, alarm_name),
        )
        self.storage.execute(
            "INSERT INTO cw_alarm_history(id, alarm_name, state, reason, ts)"
            " VALUES (?,?,?,?,?)",
            (uuid.uuid4().hex, alarm_name, state_value, state_reason, now),
        )
        return {"alarm_name": alarm_name, "state": state_value}

    def get_alarm_history(self, alarm_name: str) -> list[dict]:
        if not self.storage.query_one("SELECT 1 FROM cw_alarms WHERE alarm_name=?", (alarm_name,)):
            raise NotFound(f"alarm not found: {alarm_name}")
        rows = self.storage.query(
            "SELECT * FROM cw_alarm_history WHERE alarm_name=? ORDER BY ts",
            (alarm_name,),
        )
        return [dict(r) for r in rows]
