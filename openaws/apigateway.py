"""API Gateway-style REST API router.

Create REST APIs, define resources + methods (routes), integrate them
with Lambda functions, and invoke them over the local HTTP server.

A REST API groups resources (paths) and methods (HTTP verbs).  Each
method has an integration that maps to a Lambda function.  Invoking a
route calls the integrated Lambda with an event shaped like an API
Gateway Proxy event.

The HTTP server exposes a virtual path /apigw/<api_id>/<path...> for
invocation so tests can drive the gateway without a separate server.

Protocol for management: JSON action on POST /apigateway.
Invocation: POST/GET /apigw/<api_id>/<resource_path>
"""

from __future__ import annotations

import json
import time
import uuid
from typing import Any

from .errors import Conflict, NotFound, ValidationError
from .storage import Storage


class APIGatewayService:
    def __init__(self, storage: Storage):
        self.storage = storage
        self._lambdas: Any = None

    # ------------------------------------------------------------------
    # REST APIs
    # ------------------------------------------------------------------

    def create_rest_api(self, name: str, description: str = "") -> dict[str, Any]:
        if not name:
            raise ValidationError("API name is required")
        if self.storage.query_one("SELECT id FROM apigw_apis WHERE name=?", (name,)):
            raise Conflict(f"REST API already exists: {name}")
        api_id = uuid.uuid4().hex[:10]
        self.storage.execute(
            "INSERT INTO apigw_apis(id, name, description, created_at) VALUES (?,?,?,?)",
            (api_id, name, description, time.time()),
        )
        return {"id": api_id, "name": name, "description": description}

    def list_rest_apis(self) -> list[dict[str, Any]]:
        rows = self.storage.query("SELECT id, name, description FROM apigw_apis ORDER BY name")
        return [dict(r) for r in rows]

    def delete_rest_api(self, api_id: str) -> None:
        self._require_api(api_id)
        self.storage.execute("DELETE FROM apigw_resources WHERE api_id=?", (api_id,))
        self.storage.execute("DELETE FROM apigw_apis WHERE id=?", (api_id,))

    def _require_api(self, api_id: str) -> dict:
        row = self.storage.query_one("SELECT * FROM apigw_apis WHERE id=?", (api_id,))
        if not row:
            raise NotFound(f"no such REST API: {api_id}")
        return dict(row)

    # ------------------------------------------------------------------
    # Resources (routes)
    # ------------------------------------------------------------------

    def create_resource(
        self,
        api_id: str,
        path: str,
        http_method: str,
        integration_type: str = "lambda",
        integration_uri: str = "",
    ) -> dict[str, Any]:
        """Create (or upsert) a route: ``http_method path`` → Lambda ``integration_uri``."""
        self._require_api(api_id)
        if not path.startswith("/"):
            path = "/" + path
        http_method = http_method.upper()
        if integration_type not in ("lambda", "mock"):
            raise ValidationError("integration_type must be 'lambda' or 'mock'")
        existing = self.storage.query_one(
            "SELECT id FROM apigw_resources WHERE api_id=? AND path=? AND http_method=?",
            (api_id, path, http_method),
        )
        if existing:
            res_id = existing["id"]
            self.storage.execute(
                "UPDATE apigw_resources SET integration_type=?, integration_uri=?"
                " WHERE id=?",
                (integration_type, integration_uri, res_id),
            )
        else:
            res_id = uuid.uuid4().hex[:10]
            self.storage.execute(
                "INSERT INTO apigw_resources"
                "(id, api_id, path, http_method, integration_type, integration_uri, created_at)"
                " VALUES (?,?,?,?,?,?,?)",
                (res_id, api_id, path, http_method, integration_type, integration_uri,
                 time.time()),
            )
        return {
            "id": res_id, "api_id": api_id, "path": path,
            "http_method": http_method, "integration_type": integration_type,
            "integration_uri": integration_uri,
        }

    def list_resources(self, api_id: str) -> list[dict[str, Any]]:
        self._require_api(api_id)
        rows = self.storage.query(
            "SELECT * FROM apigw_resources WHERE api_id=? ORDER BY path, http_method",
            (api_id,),
        )
        return [dict(r) for r in rows]

    def delete_resource(self, api_id: str, resource_id: str) -> None:
        row = self.storage.query_one(
            "SELECT id FROM apigw_resources WHERE id=? AND api_id=?",
            (resource_id, api_id),
        )
        if not row:
            raise NotFound(f"no such resource: {resource_id}")
        self.storage.execute("DELETE FROM apigw_resources WHERE id=?", (resource_id,))

    # ------------------------------------------------------------------
    # Invocation
    # ------------------------------------------------------------------

    def invoke(
        self,
        api_id: str,
        http_method: str,
        path: str,
        body: str | None = None,
        query_params: dict | None = None,
        headers: dict | None = None,
    ) -> dict[str, Any]:
        """Invoke a route, returning a proxy response dict."""
        self._require_api(api_id)
        http_method = http_method.upper()
        if not path.startswith("/"):
            path = "/" + path

        # find exact match first, then path-param match
        row = self.storage.query_one(
            "SELECT * FROM apigw_resources WHERE api_id=? AND path=? AND http_method=?",
            (api_id, path, http_method),
        )
        path_params: dict[str, str] = {}
        if not row:
            row, path_params = self._match_path_template(api_id, http_method, path)
        if not row:
            raise NotFound(f"no route: {http_method} {path}")

        event = {
            "httpMethod": http_method,
            "path": path,
            "pathParameters": path_params or None,
            "queryStringParameters": query_params or None,
            "headers": headers or {},
            "body": body,
            "requestContext": {"apiId": api_id},
        }

        itype = row["integration_type"]
        iuri = row["integration_uri"]

        if itype == "mock":
            return {
                "statusCode": 200,
                "body": json.dumps({"message": "mock response"}),
                "headers": {"Content-Type": "application/json"},
            }

        if itype == "lambda":
            fn_name = iuri.split(":")[-1].split("/")[-1] if iuri else ""
            if not fn_name or not self._lambdas:
                raise ValidationError("lambda integration requires a function name")
            result = self._lambdas.invoke(fn_name, event)
            if isinstance(result, dict) and "statusCode" in result:
                return result
            return {
                "statusCode": 200,
                "body": json.dumps(result),
                "headers": {"Content-Type": "application/json"},
            }

        raise ValidationError(f"unsupported integration_type: {itype!r}")

    def _match_path_template(
        self, api_id: str, http_method: str, path: str
    ) -> tuple[Any, dict]:
        """Match a path like /users/42 against a template like /users/{id}."""
        rows = self.storage.query(
            "SELECT * FROM apigw_resources WHERE api_id=? AND http_method=?",
            (api_id, http_method),
        )
        path_parts = path.strip("/").split("/")
        for row in rows:
            template_parts = row["path"].strip("/").split("/")
            if len(template_parts) != len(path_parts):
                continue
            params: dict[str, str] = {}
            match = True
            for tp, pp in zip(template_parts, path_parts):
                if tp.startswith("{") and tp.endswith("}"):
                    params[tp[1:-1]] = pp
                elif tp != pp:
                    match = False
                    break
            if match:
                return row, params
        return None, {}
