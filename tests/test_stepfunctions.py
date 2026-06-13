"""Tests for Step Functions: state machine definition and execution."""

import json

import pytest

from openaws.errors import Conflict, NotFound, ValidationError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SIMPLE_DEF = {
    "StartAt": "Greet",
    "States": {
        "Greet": {
            "Type": "Pass",
            "Result": {"msg": "hello"},
            "End": True,
        }
    },
}

ADD_DEF = {
    "StartAt": "Add",
    "States": {
        "Add": {
            "Type": "Task",
            "Resource": "adder",
            "End": True,
        }
    },
}

ADD_SOURCE = """
def handler(event, context):
    return {"sum": event.get("a", 0) + event.get("b", 0)}
"""


# ---------------------------------------------------------------------------
# State machine CRUD
# ---------------------------------------------------------------------------

def test_create_list_delete_state_machine(app):
    sm = app.stepfunctions.create_state_machine("greeter", SIMPLE_DEF)
    assert sm["name"] == "greeter"
    assert sm["status"] == "ACTIVE"
    machines = app.stepfunctions.list_state_machines()
    assert any(m["name"] == "greeter" for m in machines)
    app.stepfunctions.delete_state_machine("greeter")
    assert all(m["name"] != "greeter" for m in app.stepfunctions.list_state_machines())


def test_describe_state_machine(app):
    app.stepfunctions.create_state_machine("m", SIMPLE_DEF)
    desc = app.stepfunctions.describe_state_machine("m")
    assert desc["definition"]["StartAt"] == "Greet"


def test_duplicate_state_machine_raises(app):
    app.stepfunctions.create_state_machine("m", SIMPLE_DEF)
    with pytest.raises(Conflict):
        app.stepfunctions.create_state_machine("m", SIMPLE_DEF)


def test_create_without_start_at_raises(app):
    with pytest.raises(ValidationError):
        app.stepfunctions.create_state_machine("bad", {"States": {"X": {"Type": "Pass", "End": True}}})


def test_delete_unknown_raises(app):
    with pytest.raises(NotFound):
        app.stepfunctions.delete_state_machine("ghost")


# ---------------------------------------------------------------------------
# Pass state
# ---------------------------------------------------------------------------

def test_pass_state_inject_result(app):
    app.stepfunctions.fast_wait = True
    app.stepfunctions.create_state_machine("p", SIMPLE_DEF)
    result = app.stepfunctions.start_execution("p")
    assert result["status"] == "SUCCEEDED"
    assert result["output"] == {"msg": "hello"}


def test_pass_state_with_result_path(app):
    defn = {
        "StartAt": "S",
        "States": {
            "S": {
                "Type": "Pass",
                "Result": "injected",
                "ResultPath": "$.added",
                "End": True,
            }
        },
    }
    app.stepfunctions.create_state_machine("m", defn)
    result = app.stepfunctions.start_execution("m", {"existing": 1})
    assert result["output"]["existing"] == 1
    assert result["output"]["added"] == "injected"


# ---------------------------------------------------------------------------
# Task state
# ---------------------------------------------------------------------------

def test_task_state_calls_lambda(app):
    app.lambdas.register_source("adder", ADD_SOURCE)
    app.stepfunctions.fast_wait = True
    app.stepfunctions.create_state_machine("calc", ADD_DEF)
    result = app.stepfunctions.start_execution("calc", {"a": 3, "b": 7})
    assert result["status"] == "SUCCEEDED"
    assert result["output"]["sum"] == 10


# ---------------------------------------------------------------------------
# Choice state
# ---------------------------------------------------------------------------

def test_choice_state_routing(app):
    defn = {
        "StartAt": "Check",
        "States": {
            "Check": {
                "Type": "Choice",
                "Choices": [
                    {
                        "Variable": "$.x",
                        "NumericGreaterThan": 10,
                        "Next": "Big",
                    },
                ],
                "Default": "Small",
            },
            "Big": {
                "Type": "Pass",
                "Result": "big",
                "End": True,
            },
            "Small": {
                "Type": "Pass",
                "Result": "small",
                "End": True,
            },
        },
    }
    app.stepfunctions.create_state_machine("chooser", defn)
    r_big = app.stepfunctions.start_execution("chooser", {"x": 20})
    assert r_big["output"] == "big"
    r_small = app.stepfunctions.start_execution("chooser", {"x": 5})
    assert r_small["output"] == "small"


def test_choice_string_equals(app):
    defn = {
        "StartAt": "C",
        "States": {
            "C": {
                "Type": "Choice",
                "Choices": [{"Variable": "$.env", "StringEquals": "prod", "Next": "P"}],
                "Default": "D",
            },
            "P": {"Type": "Pass", "Result": "production", "End": True},
            "D": {"Type": "Pass", "Result": "dev", "End": True},
        },
    }
    app.stepfunctions.create_state_machine("env-check", defn)
    assert app.stepfunctions.start_execution("env-check", {"env": "prod"})["output"] == "production"
    assert app.stepfunctions.start_execution("env-check", {"env": "dev"})["output"] == "dev"


# ---------------------------------------------------------------------------
# Wait state
# ---------------------------------------------------------------------------

def test_wait_state_fast_wait(app):
    defn = {
        "StartAt": "W",
        "States": {
            "W": {
                "Type": "Wait",
                "Seconds": 60,
                "Next": "D",
            },
            "D": {"Type": "Pass", "Result": "done", "End": True},
        },
    }
    app.stepfunctions.fast_wait = True
    app.stepfunctions.create_state_machine("waiter", defn)
    result = app.stepfunctions.start_execution("waiter", {})
    assert result["status"] == "SUCCEEDED"
    assert result["output"] == "done"


# ---------------------------------------------------------------------------
# Parallel state
# ---------------------------------------------------------------------------

def test_parallel_state(app):
    defn = {
        "StartAt": "P",
        "States": {
            "P": {
                "Type": "Parallel",
                "Branches": [
                    {
                        "StartAt": "A",
                        "States": {"A": {"Type": "Pass", "Result": "branch-a", "End": True}},
                    },
                    {
                        "StartAt": "B",
                        "States": {"B": {"Type": "Pass", "Result": "branch-b", "End": True}},
                    },
                ],
                "End": True,
            }
        },
    }
    app.stepfunctions.create_state_machine("par", defn)
    result = app.stepfunctions.start_execution("par", {})
    assert result["status"] == "SUCCEEDED"
    assert set(result["output"]) == {"branch-a", "branch-b"}


# ---------------------------------------------------------------------------
# Fail state
# ---------------------------------------------------------------------------

def test_fail_state(app):
    defn = {
        "StartAt": "F",
        "States": {
            "F": {
                "Type": "Fail",
                "Error": "MyError",
                "Cause": "Something went wrong",
            }
        },
    }
    app.stepfunctions.create_state_machine("failer", defn)
    result = app.stepfunctions.start_execution("failer", {})
    assert result["status"] == "FAILED"
    assert "error" in result


# ---------------------------------------------------------------------------
# Executions
# ---------------------------------------------------------------------------

def test_list_executions(app):
    app.stepfunctions.create_state_machine("m", SIMPLE_DEF)
    app.stepfunctions.start_execution("m")
    app.stepfunctions.start_execution("m")
    execs = app.stepfunctions.list_executions("m")
    assert len(execs) == 2


def test_describe_execution(app):
    app.stepfunctions.create_state_machine("m", SIMPLE_DEF)
    ex = app.stepfunctions.start_execution("m", {"k": "v"})
    desc = app.stepfunctions.describe_execution(ex["execution_arn"])
    assert desc["status"] == "SUCCEEDED"
    assert desc["input"]["k"] == "v"


def test_describe_execution_unknown_raises(app):
    with pytest.raises(NotFound):
        app.stepfunctions.describe_execution("arn:openaws:states:local:000:execution:x:ghost")


# ---------------------------------------------------------------------------
# HTTP server round-trip
# ---------------------------------------------------------------------------

def test_stepfunctions_via_http(server):
    import urllib.request

    base = server.base_url

    def post(payload):
        data = json.dumps(payload).encode()
        req = urllib.request.Request(
            f"{base}/stepfunctions",
            data=data,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read())

    defn = {
        "StartAt": "S",
        "States": {"S": {"Type": "Pass", "Result": {"ok": True}, "End": True}},
    }
    post({"action": "create_state_machine", "name": "test-sm", "definition": defn})

    machines = post({"action": "list_state_machines"})
    assert any(m["name"] == "test-sm" for m in machines["state_machines"])

    ex = post({"action": "start_execution", "state_machine": "test-sm"})
    assert ex["status"] == "SUCCEEDED"
    assert ex["output"] == {"ok": True}

    desc = post({"action": "describe_execution", "execution_arn": ex["execution_arn"]})
    assert desc["status"] == "SUCCEEDED"

    post({"action": "delete_state_machine", "name": "test-sm"})
    assert post({"action": "list_state_machines"})["state_machines"] == []
