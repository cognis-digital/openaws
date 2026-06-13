"""Single local HTTP server exposing every openaws service.

Routing is path-prefix based so each service lives under a clear namespace:

    /s3/...      S3 object store
    /dynamodb    DynamoDB-style table store (JSON action API)
    /sqs         SQS-style queue (JSON action API)
    /lambda      Lambda-style runner (JSON action API)
    /            health / service listing

S3 uses RESTful paths (``/s3/<bucket>/<key>``) because objects are binary;
the other three use a small JSON action protocol (POST a body containing an
``"action"`` plus its parameters) which keeps the client surface tiny while
remaining easy to script against.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlparse

from . import __version__
from .dynamodb import DynamoDBService
from .errors import OpenAWSError, ValidationError
from .lambdas import LambdaService
from .s3 import S3Service
from .sqs import SQSService
from .storage import Storage


class App:
    """Holds the service instances backed by one Storage."""

    def __init__(self, data_dir: str | None = None, storage: Storage | None = None):
        self.storage = storage or Storage(data_dir)
        self.s3 = S3Service(self.storage)
        self.dynamodb = DynamoDBService(self.storage)
        self.sqs = SQSService(self.storage)
        self.lambdas = LambdaService(self.storage)


def _dispatch_dynamodb(app: App, payload: dict):
    action = payload.get("action")
    d = app.dynamodb
    if action == "create_table":
        return d.create_table(payload["name"], payload["hash_key"], payload.get("range_key"))
    if action == "list_tables":
        return {"tables": d.list_tables()}
    if action == "describe_table":
        return d.describe_table(payload["name"])
    if action == "delete_table":
        d.delete_table(payload["name"])
        return {"deleted": payload["name"]}
    if action == "put_item":
        return {"item": d.put_item(payload["table"], payload["item"])}
    if action == "get_item":
        return {"item": d.get_item(payload["table"], payload["key"])}
    if action == "delete_item":
        d.delete_item(payload["table"], payload["key"])
        return {"deleted": True}
    if action == "query":
        return {
            "items": d.query(
                payload["table"],
                payload["key_value"],
                payload.get("sort_begins_with"),
                payload.get("sort_eq"),
            )
        }
    if action == "scan":
        return {"items": d.scan(payload["table"], payload.get("filters"))}
    raise ValidationError(f"unknown dynamodb action: {action!r}")


def _dispatch_sqs(app: App, payload: dict):
    action = payload.get("action")
    q = app.sqs
    if action == "create_queue":
        return q.create_queue(
            payload["name"], payload.get("visibility_timeout", 30.0)
        )
    if action == "list_queues":
        return {"queues": q.list_queues()}
    if action == "delete_queue":
        q.delete_queue(payload["name"])
        return {"deleted": payload["name"]}
    if action == "send_message":
        return q.send_message(payload["queue"], payload["body"])
    if action == "receive_messages":
        return {
            "messages": q.receive_messages(
                payload["queue"], payload.get("max_messages", 1)
            )
        }
    if action == "delete_message":
        return {"deleted": q.delete_message(payload["queue"], payload["receipt_handle"])}
    if action == "message_count":
        return {"count": q.message_count(payload["queue"])}
    raise ValidationError(f"unknown sqs action: {action!r}")


def _dispatch_lambda(app: App, payload: dict):
    action = payload.get("action")
    fn = app.lambdas
    if action == "register_source":
        return fn.register_source(
            payload["name"], payload["source"], payload.get("handler", "handler")
        )
    if action == "list_functions":
        return {"functions": fn.list_functions()}
    if action == "delete_function":
        fn.delete_function(payload["name"])
        return {"deleted": payload["name"]}
    if action == "invoke":
        return {"result": fn.invoke(payload["name"], payload.get("event"))}
    if action == "invoke_from_sqs":
        return {
            "results": fn.invoke_from_sqs(
                payload["name"], app.sqs, payload["queue"], payload.get("max_messages", 10)
            )
        }
    if action == "invoke_from_s3_put":
        return {
            "result": fn.invoke_from_s3_put(
                payload["name"], payload["bucket"], payload["key"], payload.get("size", 0)
            )
        }
    raise ValidationError(f"unknown lambda action: {action!r}")


def make_handler(app: App):
    class Handler(BaseHTTPRequestHandler):
        server_version = f"openaws/{__version__}"

        def log_message(self, *args):  # silence default stderr logging
            pass

        # --- response helpers ------------------------------------------
        def _send_json(self, obj, status=200):
            data = json.dumps(obj).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _send_bytes(self, data, content_type, status=200, headers=None):
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(data)))
            for k, v in (headers or {}).items():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(data)

        def _error(self, exc):
            if isinstance(exc, OpenAWSError):
                self._send_json({"error": exc.code, "message": exc.message}, exc.status)
            else:
                self._send_json({"error": "InternalError", "message": str(exc)}, 500)

        def _read_body(self):
            length = int(self.headers.get("Content-Length", 0) or 0)
            return self.rfile.read(length) if length else b""

        def _read_json(self):
            raw = self._read_body()
            if not raw:
                return {}
            try:
                return json.loads(raw.decode("utf-8"))
            except json.JSONDecodeError as exc:
                raise ValidationError(f"invalid JSON body: {exc}") from exc

        # --- routing ----------------------------------------------------
        def _route(self, method):
            parsed = urlparse(self.path)
            path = parsed.path
            try:
                if path == "/" or path == "/health":
                    return self._send_json(
                        {
                            "service": "openaws",
                            "version": __version__,
                            "services": ["s3", "dynamodb", "sqs", "lambda"],
                        }
                    )
                if path.startswith("/s3"):
                    return self._handle_s3(method, path, parsed)
                if path == "/dynamodb":
                    return self._send_json(_dispatch_dynamodb(app, self._read_json()))
                if path == "/sqs":
                    return self._send_json(_dispatch_sqs(app, self._read_json()))
                if path == "/lambda":
                    return self._send_json(_dispatch_lambda(app, self._read_json()))
                return self._send_json({"error": "NotFound", "message": path}, 404)
            except Exception as exc:  # noqa: BLE001 - convert to HTTP error
                return self._error(exc)

        def _handle_s3(self, method, path, parsed):
            # /s3                       -> list buckets (GET)
            # /s3/<bucket>              -> create/delete bucket / list objects
            # /s3/<bucket>/<key...>     -> object ops
            rest = path[len("/s3"):].lstrip("/")
            parts = rest.split("/", 1) if rest else []
            if not parts:
                if method == "GET":
                    return self._send_json({"buckets": app.s3.list_buckets()})
                raise ValidationError("unsupported /s3 operation")
            bucket = unquote(parts[0])
            key = unquote(parts[1]) if len(parts) > 1 and parts[1] else None
            if key is None:
                if method == "PUT":
                    return self._send_json(app.s3.create_bucket(bucket), 201)
                if method == "DELETE":
                    app.s3.delete_bucket(bucket)
                    return self._send_json({"deleted": bucket})
                if method == "GET":
                    qs = parse_qs(parsed.query)
                    prefix = qs.get("prefix", [""])[0]
                    return self._send_json(
                        {"objects": app.s3.list_objects(bucket, prefix)}
                    )
                raise ValidationError("unsupported bucket operation")
            if method == "PUT":
                body = self._read_body()
                ctype = self.headers.get("Content-Type", "application/octet-stream")
                return self._send_json(app.s3.put_object(bucket, key, body, ctype), 201)
            if method == "GET":
                obj = app.s3.get_object(bucket, key)
                return self._send_bytes(
                    obj["body"], obj["content_type"], headers={"ETag": obj["etag"]}
                )
            if method == "DELETE":
                app.s3.delete_object(bucket, key)
                return self._send_json({"deleted": key})
            raise ValidationError("unsupported object operation")

        def do_GET(self):
            self._route("GET")

        def do_PUT(self):
            self._route("PUT")

        def do_POST(self):
            self._route("POST")

        def do_DELETE(self):
            self._route("DELETE")

    return Handler


class OpenAWSServer:
    """A ThreadingHTTPServer wrapper that can run in-process for tests."""

    def __init__(self, host: str = "127.0.0.1", port: int = 0, data_dir: str | None = None,
                 app: App | None = None):
        self.app = app or App(data_dir)
        self._httpd = ThreadingHTTPServer((host, port), make_handler(self.app))
        self._thread: threading.Thread | None = None

    @property
    def host(self) -> str:
        return self._httpd.server_address[0]

    @property
    def port(self) -> int:
        return self._httpd.server_address[1]

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    def start(self) -> "OpenAWSServer":
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)
        self._thread.start()
        return self

    def serve_forever(self):  # pragma: no cover - blocking entry for CLI
        self._httpd.serve_forever()

    def stop(self):
        self._httpd.shutdown()
        self._httpd.server_close()
        if self._thread:
            self._thread.join(timeout=2)
