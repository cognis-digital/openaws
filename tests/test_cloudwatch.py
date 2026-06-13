"""Tests for CloudWatchService — metrics, logs, alarms."""
import time
import pytest

from openaws.errors import Conflict, NotFound, ValidationError


# ===========================================================================
# Metrics
# ===========================================================================

def test_put_and_list_metrics(app):
    app.cloudwatch.put_metric_data("MyApp", [
        {"MetricName": "Latency", "Value": 42.0, "Unit": "Milliseconds"},
        {"MetricName": "Errors", "Value": 1.0, "Unit": "Count"},
    ])
    metrics = app.cloudwatch.list_metrics("MyApp")
    names = [m["metric_name"] for m in metrics]
    assert "Latency" in names
    assert "Errors" in names


def test_get_metric_statistics(app):
    now = time.time()
    for v in [10.0, 20.0, 30.0]:
        app.cloudwatch.put_metric_data("NS", [
            {"MetricName": "Req", "Value": v, "Unit": "Count", "Timestamp": now}
        ])
    dp = app.cloudwatch.get_metric_statistics(
        "NS", "Req", now - 1, now + 1, period=60, statistics=["Average", "Sum", "Maximum"]
    )
    assert len(dp) > 0
    assert dp[0]["Average"] == 20.0
    assert dp[0]["Sum"] == 60.0
    assert dp[0]["Maximum"] == 30.0


def test_get_metric_statistics_empty(app):
    now = time.time()
    dp = app.cloudwatch.get_metric_statistics("Empty", "Missing", now - 10, now, period=60)
    assert dp == []


def test_list_metrics_filter_by_name(app):
    app.cloudwatch.put_metric_data("Svc", [{"MetricName": "Hits", "Value": 1.0}])
    app.cloudwatch.put_metric_data("Svc", [{"MetricName": "Misses", "Value": 2.0}])
    metrics = app.cloudwatch.list_metrics("Svc", "Hits")
    assert len(metrics) == 1
    assert metrics[0]["metric_name"] == "Hits"


def test_put_metric_requires_namespace(app):
    with pytest.raises(ValidationError):
        app.cloudwatch.put_metric_data("", [{"MetricName": "X", "Value": 1.0}])


def test_put_metric_requires_metric_name(app):
    with pytest.raises(ValidationError):
        app.cloudwatch.put_metric_data("NS", [{"Value": 1.0}])


# ===========================================================================
# Log Groups
# ===========================================================================

def test_create_list_delete_log_group(app):
    app.cloudwatch.create_log_group("/aws/lambda/my-fn")
    groups = app.cloudwatch.list_log_groups()
    names = [g["log_group_name"] for g in groups]
    assert "/aws/lambda/my-fn" in names
    app.cloudwatch.delete_log_group("/aws/lambda/my-fn")
    groups_after = app.cloudwatch.list_log_groups()
    assert "/aws/lambda/my-fn" not in [g["log_group_name"] for g in groups_after]


def test_duplicate_log_group_raises(app):
    app.cloudwatch.create_log_group("/dup")
    with pytest.raises(Conflict):
        app.cloudwatch.create_log_group("/dup")


def test_list_log_groups_prefix(app):
    app.cloudwatch.create_log_group("/app/svc-a")
    app.cloudwatch.create_log_group("/app/svc-b")
    app.cloudwatch.create_log_group("/other/x")
    groups = app.cloudwatch.list_log_groups(prefix="/app")
    names = [g["log_group_name"] for g in groups]
    assert "/app/svc-a" in names
    assert "/app/svc-b" in names
    assert "/other/x" not in names


# ===========================================================================
# Log Streams
# ===========================================================================

def test_create_list_delete_log_stream(app):
    app.cloudwatch.create_log_group("/svc/logs")
    app.cloudwatch.create_log_stream("/svc/logs", "stream-001")
    streams = app.cloudwatch.list_log_streams("/svc/logs")
    names = [s["log_stream_name"] for s in streams]
    assert "stream-001" in names
    app.cloudwatch.delete_log_stream("/svc/logs", "stream-001")
    assert app.cloudwatch.list_log_streams("/svc/logs") == []


def test_create_stream_in_nonexistent_group_raises(app):
    with pytest.raises(NotFound):
        app.cloudwatch.create_log_stream("/no/such/group", "s")


# ===========================================================================
# Log Events
# ===========================================================================

def test_put_and_get_log_events(app):
    app.cloudwatch.create_log_group("/g")
    app.cloudwatch.create_log_stream("/g", "s")
    now = time.time()
    app.cloudwatch.put_log_events("/g", "s", [
        {"timestamp": now, "message": "line 1"},
        {"timestamp": now + 1, "message": "line 2"},
    ])
    result = app.cloudwatch.get_log_events("/g", "s")
    messages = [e["message"] for e in result["events"]]
    assert "line 1" in messages
    assert "line 2" in messages


def test_get_log_events_time_filter(app):
    app.cloudwatch.create_log_group("/tf")
    app.cloudwatch.create_log_stream("/tf", "s")
    base = 1_700_000_000.0
    app.cloudwatch.put_log_events("/tf", "s", [
        {"timestamp": base, "message": "old"},
        {"timestamp": base + 3600, "message": "new"},
    ])
    result = app.cloudwatch.get_log_events("/tf", "s", start_time=base + 1800)
    messages = [e["message"] for e in result["events"]]
    assert "old" not in messages
    assert "new" in messages


def test_filter_log_events(app):
    app.cloudwatch.create_log_group("/filter")
    app.cloudwatch.create_log_stream("/filter", "s")
    app.cloudwatch.put_log_events("/filter", "s", [
        {"message": "ERROR something went wrong"},
        {"message": "INFO all good"},
    ])
    result = app.cloudwatch.filter_log_events("/filter", filter_pattern="ERROR")
    messages = [e["message"] for e in result["events"]]
    assert any("ERROR" in m for m in messages)
    assert all("INFO" not in m for m in messages)


# ===========================================================================
# Alarms
# ===========================================================================

def test_put_and_describe_alarm(app):
    app.cloudwatch.put_metric_alarm(
        alarm_name="high-latency",
        namespace="MyApp",
        metric_name="Latency",
        comparison_operator="GreaterThanThreshold",
        threshold=100.0,
    )
    alarm = app.cloudwatch.describe_alarm("high-latency")
    assert alarm["alarm_name"] == "high-latency"
    assert alarm["state"] == "INSUFFICIENT_DATA"
    assert alarm["threshold"] == 100.0


def test_list_alarms(app):
    app.cloudwatch.put_metric_alarm("a1", "NS", "M1", "GreaterThanThreshold", 10.0)
    app.cloudwatch.put_metric_alarm("a2", "NS", "M2", "LessThanThreshold", 5.0)
    alarms = app.cloudwatch.describe_alarms()
    names = [a["alarm_name"] for a in alarms]
    assert "a1" in names and "a2" in names


def test_filter_alarms_by_state(app):
    app.cloudwatch.put_metric_alarm("b1", "NS", "M", "GreaterThanThreshold", 1.0)
    app.cloudwatch.set_alarm_state("b1", "ALARM")
    alarms = app.cloudwatch.describe_alarms(state_value="ALARM")
    assert any(a["alarm_name"] == "b1" for a in alarms)
    ok_alarms = app.cloudwatch.describe_alarms(state_value="OK")
    assert not any(a["alarm_name"] == "b1" for a in ok_alarms)


def test_set_alarm_state(app):
    app.cloudwatch.put_metric_alarm("c1", "NS", "M", "GreaterThanThreshold", 5.0)
    result = app.cloudwatch.set_alarm_state("c1", "OK", state_reason="test override")
    assert result["state"] == "OK"
    alarm = app.cloudwatch.describe_alarm("c1")
    assert alarm["state"] == "OK"


def test_alarm_history(app):
    app.cloudwatch.put_metric_alarm("d1", "NS", "M", "GreaterThanThreshold", 5.0)
    app.cloudwatch.set_alarm_state("d1", "ALARM", "went alarm")
    app.cloudwatch.set_alarm_state("d1", "OK", "recovered")
    history = app.cloudwatch.get_alarm_history("d1")
    states = [h["state"] for h in history]
    assert "ALARM" in states
    assert "OK" in states


def test_delete_alarm(app):
    app.cloudwatch.put_metric_alarm("del-me", "NS", "M", "GreaterThanThreshold", 1.0)
    app.cloudwatch.delete_alarm("del-me")
    with pytest.raises(NotFound):
        app.cloudwatch.describe_alarm("del-me")


def test_put_alarm_upsert(app):
    app.cloudwatch.put_metric_alarm("up1", "NS", "M", "GreaterThanThreshold", 10.0)
    app.cloudwatch.put_metric_alarm("up1", "NS", "M", "GreaterThanThreshold", 20.0)
    alarm = app.cloudwatch.describe_alarm("up1")
    assert alarm["threshold"] == 20.0


def test_invalid_comparison_operator(app):
    with pytest.raises(ValidationError):
        app.cloudwatch.put_metric_alarm("bad", "NS", "M", "InvalidOp", 1.0)


def test_cloudwatch_http_roundtrip(server):
    import urllib.request, json, time as _time
    base = server.base_url + "/cloudwatch"

    def call(payload):
        data = json.dumps(payload).encode()
        req = urllib.request.Request(base, data=data,
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read())

    now = _time.time()
    call({"action": "put_metric_data", "namespace": "HTTP",
          "metric_data": [{"MetricName": "Reqs", "Value": 5.0, "Timestamp": now}]})

    stats = call({"action": "get_metric_statistics", "namespace": "HTTP",
                  "metric_name": "Reqs", "start_time": now - 1, "end_time": now + 1,
                  "statistics": ["Sum"]})
    assert stats["datapoints"][0]["Sum"] == 5.0

    call({"action": "create_log_group", "log_group_name": "/http/test"})
    call({"action": "create_log_stream", "log_group_name": "/http/test", "log_stream_name": "s"})
    call({"action": "put_log_events", "log_group_name": "/http/test",
          "log_stream_name": "s", "log_events": [{"message": "hello", "timestamp": now}]})
    events = call({"action": "get_log_events", "log_group_name": "/http/test",
                   "log_stream_name": "s"})
    assert any(e["message"] == "hello" for e in events["events"])
