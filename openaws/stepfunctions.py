"""Step Functions-style state machine runner.

Supports a subset of Amazon States Language (ASL):
  - Task  — invokes a Lambda function (Resource = function name or ARN last segment)
  - Pass  — pass input to output, optionally inject result
  - Choice — branch on variable comparisons
  - Wait   — sleep for Seconds (capped to 0 in tests via fast_wait flag)
  - Parallel — run branches concurrently, collect results as list
  - Succeed / Fail — terminal states

Executions are synchronous; the entire machine runs in-process and
returns the final output.  State-machine definitions and execution
histories are stored in SQLite.

Protocol: JSON action on POST /stepfunctions.
"""

from __future__ import annotations

import copy
import json
import re
import threading
import time
import uuid
from typing import Any

from .errors import Conflict, NotFound, ValidationError
from .storage import Storage


# ---------------------------------------------------------------------------
# ASL interpreter
# ---------------------------------------------------------------------------

def _get_path(data: Any, path: str) -> Any:
    """Retrieve a value from *data* using a JSONPath-like ``$.key.sub`` reference."""
    if not path.startswith("$"):
        return path
    parts = re.split(r"\.", path[2:]) if path.startswith("$.") else []
    cur = data
    for p in parts:
        if p == "":
            continue
        if isinstance(cur, dict):
            cur = cur.get(p)
        else:
            return None
    return cur


def _set_path(data: dict, path: str, value: Any) -> None:
    """Set a value in *data* at a ``$.key.sub`` reference."""
    if not path.startswith("$."):
        return
    parts = path[2:].split(".")
    cur = data
    for p in parts[:-1]:
        if p not in cur or not isinstance(cur[p], dict):
            cur[p] = {}
        cur = cur[p]
    cur[parts[-1]] = value


def _compare(variable: Any, op: str, value: Any) -> bool:
    if op == "StringEquals":
        return variable == value
    if op == "StringNotEquals":
        return variable != value
    if op == "StringLessThan":
        return isinstance(variable, str) and variable < value
    if op == "StringGreaterThan":
        return isinstance(variable, str) and variable > value
    if op == "NumericEquals":
        return variable == value
    if op == "NumericNotEquals":
        return variable != value
    if op == "NumericLessThan":
        return isinstance(variable, (int, float)) and variable < value
    if op == "NumericGreaterThan":
        return isinstance(variable, (int, float)) and variable > value
    if op == "NumericLessThanEquals":
        return isinstance(variable, (int, float)) and variable <= value
    if op == "NumericGreaterThanEquals":
        return isinstance(variable, (int, float)) and variable >= value
    if op == "BooleanEquals":
        return variable == value
    if op == "IsNull":
        return variable is None
    if op == "IsPresent":
        return variable is not None
    if op == "IsString":
        return isinstance(variable, str)
    if op == "IsNumeric":
        return isinstance(variable, (int, float))
    if op == "IsBoolean":
        return isinstance(variable, bool)
    return False


def _eval_condition(rule: dict, data: dict) -> bool:
    """Evaluate a single Choice rule condition."""
    if "And" in rule:
        return all(_eval_condition(c, data) for c in rule["And"])
    if "Or" in rule:
        return any(_eval_condition(c, data) for c in rule["Or"])
    if "Not" in rule:
        return not _eval_condition(rule["Not"], data)
    var_path = rule.get("Variable", "$")
    var_val = _get_path(data, var_path)
    for op in (
        "StringEquals", "StringNotEquals", "StringLessThan", "StringGreaterThan",
        "NumericEquals", "NumericNotEquals", "NumericLessThan", "NumericGreaterThan",
        "NumericLessThanEquals", "NumericGreaterThanEquals",
        "BooleanEquals", "IsNull", "IsPresent", "IsString", "IsNumeric", "IsBoolean",
    ):
        if op in rule:
            return _compare(var_val, op, rule[op])
    return False


def _apply_result_path(input_data: dict, result: Any, result_path: str | None) -> dict:
    """Merge *result* into a copy of *input_data* at *result_path*."""
    if result_path is None or result_path == "$":
        if isinstance(result, dict):
            return result
        return result
    out = copy.deepcopy(input_data) if isinstance(input_data, dict) else {}
    _set_path(out, result_path, result)
    return out


class _Interpreter:
    def __init__(self, definition: dict, lambdas: Any = None, fast_wait: bool = False):
        self.states = definition.get("States", {})
        self.start = definition.get("StartAt")
        self.lambdas = lambdas
        self.fast_wait = fast_wait

    def run(self, input_data: Any) -> Any:
        if not self.start:
            raise ValidationError("state machine has no StartAt")
        return self._run_state(self.start, input_data)

    def _run_state(self, state_name: str, data: Any) -> Any:
        visited = 0
        current_name = state_name
        current_data = data
        while True:
            visited += 1
            if visited > 1000:
                raise ValidationError("state machine execution limit exceeded (cycle?)")
            state = self.states.get(current_name)
            if state is None:
                raise ValidationError(f"no such state: {current_name!r}")
            stype = state.get("Type")
            if stype == "Task":
                current_data = self._exec_task(current_name, state, current_data)
            elif stype == "Pass":
                current_data = self._exec_pass(state, current_data)
            elif stype == "Wait":
                current_data = self._exec_wait(state, current_data)
            elif stype == "Choice":
                current_name = self._exec_choice(state, current_data)
                continue
            elif stype == "Parallel":
                current_data = self._exec_parallel(state, current_data)
            elif stype == "Succeed":
                return current_data
            elif stype == "Fail":
                cause = state.get("Cause", "StateFailed")
                error = state.get("Error", "States.TaskFailed")
                raise ValidationError(f"State machine failed: {error}: {cause}")
            else:
                raise ValidationError(f"unsupported state type: {stype!r}")
            nxt = state.get("Next")
            if state.get("End") or not nxt:
                return current_data
            current_name = nxt

    def _exec_task(self, name: str, state: dict, data: Any) -> Any:
        resource = state.get("Resource", "")
        fn_name = resource.split(":")[-1].split("/")[-1] if resource else name
        if self.lambdas:
            result = self.lambdas.invoke(fn_name, data)
        else:
            result = data  # no-op if no lambda service
        rp = state.get("ResultPath", "$")
        return _apply_result_path(
            data if isinstance(data, dict) else {}, result, rp
        )

    def _exec_pass(self, state: dict, data: Any) -> Any:
        result = state.get("Result", data)
        rp = state.get("ResultPath", "$")
        return _apply_result_path(
            data if isinstance(data, dict) else {}, result, rp
        )

    def _exec_wait(self, state: dict, data: Any) -> Any:
        seconds = state.get("Seconds", 0)
        if not self.fast_wait and seconds > 0:
            time.sleep(min(float(seconds), 30.0))
        return data

    def _exec_choice(self, state: dict, data: Any) -> str:
        for rule in state.get("Choices", []):
            if _eval_condition(rule, data):
                return rule["Next"]
        default = state.get("Default")
        if not default:
            raise ValidationError("Choice state: no matching rule and no Default")
        return default

    def _exec_parallel(self, state: dict, data: Any) -> list:
        branches = state.get("Branches", [])
        results = [None] * len(branches)
        errors: list[Exception] = []

        def _run_branch(idx: int, branch_def: dict):
            try:
                interp = _Interpreter(branch_def, self.lambdas, self.fast_wait)
                results[idx] = interp.run(copy.deepcopy(data))
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [
            threading.Thread(target=_run_branch, args=(i, b))
            for i, b in enumerate(branches)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        if errors:
            raise errors[0]
        rp = state.get("ResultPath", "$")
        return _apply_result_path(
            data if isinstance(data, dict) else {}, results, rp
        )


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------

class StepFunctionsService:
    def __init__(self, storage: Storage):
        self.storage = storage
        self._lambdas: Any = None
        self.fast_wait: bool = False  # set to True in tests

    # ------------------------------------------------------------------
    # State machines
    # ------------------------------------------------------------------

    def create_state_machine(self, name: str, definition: dict) -> dict[str, Any]:
        if not name:
            raise ValidationError("state machine name is required")
        if not definition.get("StartAt"):
            raise ValidationError("definition must have StartAt")
        if not definition.get("States"):
            raise ValidationError("definition must have States")
        if self.storage.query_one("SELECT name FROM sf_state_machines WHERE name=?", (name,)):
            raise Conflict(f"state machine already exists: {name}")
        arn = f"arn:openaws:states:local:000000000000:stateMachine:{name}"
        self.storage.execute(
            "INSERT INTO sf_state_machines(name, arn, definition_json, status, created_at)"
            " VALUES (?,?,?,?,?)",
            (name, arn, json.dumps(definition), "ACTIVE", time.time()),
        )
        return {"name": name, "arn": arn, "status": "ACTIVE"}

    def list_state_machines(self) -> list[dict[str, Any]]:
        rows = self.storage.query(
            "SELECT name, arn, status FROM sf_state_machines ORDER BY name"
        )
        return [dict(r) for r in rows]

    def describe_state_machine(self, name: str) -> dict[str, Any]:
        row = self._require_sm(name)
        out = dict(row)
        out["definition"] = json.loads(out.pop("definition_json"))
        return out

    def delete_state_machine(self, name: str) -> None:
        self._require_sm(name)
        self.storage.execute("DELETE FROM sf_executions WHERE state_machine=?", (name,))
        self.storage.execute("DELETE FROM sf_state_machines WHERE name=?", (name,))

    def _require_sm(self, name: str) -> dict:
        row = self.storage.query_one(
            "SELECT * FROM sf_state_machines WHERE name=?", (name,)
        )
        if not row:
            raise NotFound(f"no such state machine: {name}")
        return dict(row)

    # ------------------------------------------------------------------
    # Executions
    # ------------------------------------------------------------------

    def start_execution(
        self,
        state_machine: str,
        input_data: Any = None,
        execution_name: str | None = None,
    ) -> dict[str, Any]:
        sm = self._require_sm(state_machine)
        definition = json.loads(sm["definition_json"])
        exec_id = execution_name or uuid.uuid4().hex
        arn = (
            f"arn:openaws:states:local:000000000000:execution:"
            f"{state_machine}:{exec_id}"
        )
        started_at = time.time()
        try:
            interp = _Interpreter(definition, self._lambdas, self.fast_wait)
            output = interp.run(input_data or {})
            status = "SUCCEEDED"
            error = None
            cause = None
        except Exception as exc:  # noqa: BLE001
            output = None
            status = "FAILED"
            error = "States.TaskFailed"
            cause = str(exc)
        stopped_at = time.time()
        self.storage.execute(
            "INSERT INTO sf_executions"
            "(exec_id, arn, state_machine, status, input_json, output_json,"
            " error, cause, started_at, stopped_at)"
            " VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                exec_id, arn, state_machine, status,
                json.dumps(input_data or {}),
                json.dumps(output) if output is not None else None,
                error, cause, started_at, stopped_at,
            ),
        )
        result = {
            "execution_arn": arn,
            "state_machine": state_machine,
            "status": status,
            "started_at": started_at,
            "stopped_at": stopped_at,
        }
        if status == "SUCCEEDED":
            result["output"] = output
        else:
            result["error"] = error
            result["cause"] = cause
        return result

    def list_executions(self, state_machine: str) -> list[dict[str, Any]]:
        self._require_sm(state_machine)
        rows = self.storage.query(
            "SELECT exec_id, arn, status, started_at, stopped_at"
            " FROM sf_executions WHERE state_machine=? ORDER BY started_at DESC",
            (state_machine,),
        )
        return [dict(r) for r in rows]

    def describe_execution(self, execution_arn: str) -> dict[str, Any]:
        row = self.storage.query_one(
            "SELECT * FROM sf_executions WHERE arn=?", (execution_arn,)
        )
        if not row:
            raise NotFound(f"no such execution: {execution_arn}")
        out = dict(row)
        if out.get("input_json"):
            out["input"] = json.loads(out.pop("input_json"))
        if out.get("output_json"):
            out["output"] = json.loads(out.pop("output_json"))
        else:
            out.pop("output_json", None)
        return out
