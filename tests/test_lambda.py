import pytest

from openaws.errors import Conflict, NotFound, ValidationError

SOURCE = """
def handler(event, context):
    return {"sum": event.get("a", 0) + event.get("b", 0), "fn": context.function_name}
"""


def test_register_and_invoke_callable(app):
    app.lambdas.register_callable("echo", lambda e, c: {"echo": e})
    out = app.lambdas.invoke("echo", {"x": 1})
    assert out == {"echo": {"x": 1}}


def test_register_and_invoke_source(app):
    app.lambdas.register_source("adder", SOURCE)
    out = app.lambdas.invoke("adder", {"a": 2, "b": 3})
    assert out["sum"] == 5
    assert out["fn"] == "adder"


def test_list_and_delete_functions(app):
    app.lambdas.register_callable("c1", lambda e, c: 1)
    app.lambdas.register_source("s1", SOURCE)
    assert app.lambdas.list_functions() == ["c1", "s1"]
    app.lambdas.delete_function("c1")
    app.lambdas.delete_function("s1")
    assert app.lambdas.list_functions() == []


def test_duplicate_function_conflicts(app):
    app.lambdas.register_callable("f", lambda e, c: 1)
    with pytest.raises(Conflict):
        app.lambdas.register_callable("f", lambda e, c: 2)


def test_bad_source_rejected(app):
    with pytest.raises(ValidationError):
        app.lambdas.register_source("bad", "this is not python !!!")
    with pytest.raises(ValidationError):
        app.lambdas.register_source("nohandler", "x = 1", handler="handler")


def test_invoke_missing_function(app):
    with pytest.raises(NotFound):
        app.lambdas.invoke("ghost")


def test_invoke_from_sqs_consumes_messages(app):
    app.sqs.create_queue("work")
    app.sqs.send_message("work", "task-1")
    app.sqs.send_message("work", "task-2")
    seen = []
    app.lambdas.register_callable(
        "worker", lambda e, c: seen.extend(r["body"] for r in e["Records"])
    )
    app.lambdas.invoke_from_sqs("worker", app.sqs, "work")
    assert sorted(seen) == ["task-1", "task-2"]
    # messages were deleted on success
    assert app.sqs.message_count("work") == 0


def test_invoke_from_sqs_empty_queue(app):
    app.sqs.create_queue("work")
    app.lambdas.register_callable("worker", lambda e, c: "ran")
    assert app.lambdas.invoke_from_sqs("worker", app.sqs, "work") == []


def test_invoke_from_s3_put_event(app):
    captured = {}

    def handler(event, context):
        rec = event["Records"][0]
        captured.update(rec["s3"]["object"])
        return rec["eventName"]

    app.lambdas.register_callable("on_put", handler)
    name = app.lambdas.invoke_from_s3_put("on_put", "bucket", "file.txt", size=12)
    assert name == "ObjectCreated:Put"
    assert captured == {"key": "file.txt", "size": 12}


def test_source_function_persists_across_resolve(app):
    app.lambdas.register_source("adder", SOURCE)
    # invoke twice; each resolve rebuilds from stored source
    assert app.lambdas.invoke("adder", {"a": 1, "b": 1})["sum"] == 2
    assert app.lambdas.invoke("adder", {"a": 10, "b": 5})["sum"] == 15
