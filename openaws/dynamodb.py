"""DynamoDB-style table store.

A real key/value document store keyed on a partition (hash) key and an
optional sort (range) key. Items are arbitrary JSON documents. Supports
put/get/delete plus query (by partition key, with optional sort-key
conditions) and scan (full-table with simple attribute equality filters).

Compatible SUBSET: secondary indexes, conditional writes, and the full
expression language are roadmap items, not implemented.
"""

from __future__ import annotations

import json
import time
from typing import Any

from .errors import Conflict, NotFound, ValidationError
from .storage import Storage

_SENTINEL_NO_SK = "\x00"  # stored sort-key value for tables without a range key


class DynamoDBService:
    def __init__(self, storage: Storage):
        self.storage = storage

    # --- tables ------------------------------------------------------------
    def create_table(
        self, name: str, hash_key: str, range_key: str | None = None
    ) -> dict[str, Any]:
        if not name or not hash_key:
            raise ValidationError("table name and hash_key are required")
        if self.storage.query_one("SELECT name FROM ddb_tables WHERE name=?", (name,)):
            raise Conflict(f"table already exists: {name}")
        self.storage.execute(
            "INSERT INTO ddb_tables(name,hash_key,range_key,created_at) VALUES (?,?,?,?)",
            (name, hash_key, range_key, time.time()),
        )
        return {"name": name, "hash_key": hash_key, "range_key": range_key}

    def list_tables(self) -> list[str]:
        rows = self.storage.query("SELECT name FROM ddb_tables ORDER BY name")
        return [r["name"] for r in rows]

    def describe_table(self, name: str) -> dict[str, Any]:
        row = self._require_table(name)
        return {"name": row["name"], "hash_key": row["hash_key"], "range_key": row["range_key"]}

    def delete_table(self, name: str) -> None:
        self._require_table(name)
        self.storage.execute("DELETE FROM ddb_items WHERE table_name=?", (name,))
        self.storage.execute("DELETE FROM ddb_tables WHERE name=?", (name,))

    def _require_table(self, name: str):
        row = self.storage.query_one("SELECT * FROM ddb_tables WHERE name=?", (name,))
        if not row:
            raise NotFound(f"no such table: {name}")
        return row

    def _keys_for(self, table_row, item: dict[str, Any]) -> tuple[str, str]:
        hk = table_row["hash_key"]
        if hk not in item:
            raise ValidationError(f"item missing partition key {hk!r}")
        pk = str(item[hk])
        rk = table_row["range_key"]
        if rk:
            if rk not in item:
                raise ValidationError(f"item missing sort key {rk!r}")
            sk = str(item[rk])
        else:
            sk = _SENTINEL_NO_SK
        return pk, sk

    # --- items -------------------------------------------------------------
    def put_item(self, table: str, item: dict[str, Any]) -> dict[str, Any]:
        row = self._require_table(table)
        if not isinstance(item, dict):
            raise ValidationError("item must be a dict")
        pk, sk = self._keys_for(row, item)
        self.storage.execute(
            """INSERT INTO ddb_items(table_name,pk,sk,item_json) VALUES (?,?,?,?)
               ON CONFLICT(table_name,pk,sk) DO UPDATE SET item_json=excluded.item_json""",
            (table, pk, sk, json.dumps(item)),
        )
        return dict(item)

    def get_item(self, table: str, key: dict[str, Any]) -> dict[str, Any] | None:
        row = self._require_table(table)
        pk, sk = self._keys_for(row, key)
        found = self.storage.query_one(
            "SELECT item_json FROM ddb_items WHERE table_name=? AND pk=? AND sk=?",
            (table, pk, sk),
        )
        return json.loads(found["item_json"]) if found else None

    def delete_item(self, table: str, key: dict[str, Any]) -> None:
        row = self._require_table(table)
        pk, sk = self._keys_for(row, key)
        self.storage.execute(
            "DELETE FROM ddb_items WHERE table_name=? AND pk=? AND sk=?", (table, pk, sk)
        )

    def query(
        self,
        table: str,
        key_value: Any,
        sort_begins_with: str | None = None,
        sort_eq: Any | None = None,
    ) -> list[dict[str, Any]]:
        """Return items with the given partition key value, ordered by sort key."""
        self._require_table(table)
        rows = self.storage.query(
            "SELECT sk,item_json FROM ddb_items WHERE table_name=? AND pk=? ORDER BY sk",
            (table, str(key_value)),
        )
        out = []
        for r in rows:
            if sort_eq is not None and r["sk"] != str(sort_eq):
                continue
            if sort_begins_with is not None and not r["sk"].startswith(sort_begins_with):
                continue
            out.append(json.loads(r["item_json"]))
        return out

    def scan(self, table: str, filters: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """Full-table scan with optional attribute-equality filters."""
        self._require_table(table)
        rows = self.storage.query(
            "SELECT item_json FROM ddb_items WHERE table_name=? ORDER BY pk,sk", (table,)
        )
        items = [json.loads(r["item_json"]) for r in rows]
        if filters:
            items = [it for it in items if all(it.get(k) == v for k, v in filters.items())]
        return items
