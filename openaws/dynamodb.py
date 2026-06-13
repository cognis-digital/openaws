"""DynamoDB-style table store.

A real key/value document store keyed on a partition (hash) key and an
optional sort (range) key. Items are arbitrary JSON documents.

This pass adds:
  - Global Secondary Indexes (GSI) and Local Secondary Indexes (LSI):
    define at table creation; query against an index name.
  - Conditional writes: ConditionExpression-style attribute_exists /
    attribute_not_exists / attribute_equals checks on put/delete.
  - BatchGetItem: fetch up to 100 items across tables in one call.
  - BatchWriteItem: put/delete up to 25 items across tables in one call.
  - TransactWriteItems: all-or-nothing multi-item write (put/delete/update),
    with optional per-operation condition checks.
  - TTL: each table can designate one attribute as the expiry timestamp;
    expired items are filtered out of all read operations.
  - UpdateExpression subset: SET (=), REMOVE, ADD (numeric increment) — the
    three most common DynamoDB update operations.
"""

from __future__ import annotations

import json
import time
from typing import Any

from .errors import Conflict, NotFound, ValidationError
from .storage import Storage

_SENTINEL_NO_SK = "\x00"  # stored sort-key value for tables without a range key

# ConditionExpression supported functions
_COND_FUNCS = frozenset(["attribute_exists", "attribute_not_exists", "attribute_equals"])


class DynamoDBService:
    def __init__(self, storage: Storage):
        self.storage = storage

    # --- tables ------------------------------------------------------------
    def create_table(
        self,
        name: str,
        hash_key: str,
        range_key: str | None = None,
        global_secondary_indexes: list[dict[str, Any]] | None = None,
        local_secondary_indexes: list[dict[str, Any]] | None = None,
        ttl_attribute: str | None = None,
    ) -> dict[str, Any]:
        """Create a table with optional GSIs, LSIs, and TTL configuration.

        GSI/LSI dicts have keys: name, hash_key, range_key (optional for GSI).
        LSI shares the table's hash_key and adds an alternate sort key.
        """
        if not name or not hash_key:
            raise ValidationError("table name and hash_key are required")
        if self.storage.query_one("SELECT name FROM ddb_tables WHERE name=?", (name,)):
            raise Conflict(f"table already exists: {name}")
        gsi_json = json.dumps(global_secondary_indexes or [])
        lsi_json = json.dumps(local_secondary_indexes or [])
        self.storage.execute(
            """INSERT INTO ddb_tables(name,hash_key,range_key,created_at,gsi_json,lsi_json,ttl_attribute)
               VALUES (?,?,?,?,?,?,?)""",
            (name, hash_key, range_key, time.time(), gsi_json, lsi_json, ttl_attribute),
        )
        return {
            "name": name,
            "hash_key": hash_key,
            "range_key": range_key,
            "global_secondary_indexes": global_secondary_indexes or [],
            "local_secondary_indexes": local_secondary_indexes or [],
            "ttl_attribute": ttl_attribute,
        }

    def list_tables(self) -> list[str]:
        rows = self.storage.query("SELECT name FROM ddb_tables ORDER BY name")
        return [r["name"] for r in rows]

    def describe_table(self, name: str) -> dict[str, Any]:
        row = self._require_table(name)
        return {
            "name": row["name"],
            "hash_key": row["hash_key"],
            "range_key": row["range_key"],
            "global_secondary_indexes": json.loads(row["gsi_json"] or "[]"),
            "local_secondary_indexes": json.loads(row["lsi_json"] or "[]"),
            "ttl_attribute": row["ttl_attribute"],
        }

    def delete_table(self, name: str) -> None:
        self._require_table(name)
        self.storage.execute("DELETE FROM ddb_items WHERE table_name=?", (name,))
        self.storage.execute("DELETE FROM ddb_tables WHERE name=?", (name,))

    def update_ttl(self, table: str, ttl_attribute: str | None) -> dict[str, Any]:
        """Enable (set attribute name) or disable (pass None) TTL on a table."""
        self._require_table(table)
        self.storage.execute(
            "UPDATE ddb_tables SET ttl_attribute=? WHERE name=?", (ttl_attribute, table)
        )
        return {"table": table, "ttl_attribute": ttl_attribute}

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

    def _is_expired(self, table_row, item: dict[str, Any]) -> bool:
        """Return True if the item has a TTL attribute that is in the past."""
        ttl_attr = table_row["ttl_attribute"]
        if not ttl_attr:
            return False
        ttl_val = item.get(ttl_attr)
        if ttl_val is None:
            return False
        try:
            return float(ttl_val) < time.time()
        except (TypeError, ValueError):
            return False

    # --- condition expressions ---------------------------------------------
    def _check_condition(
        self,
        condition: dict[str, Any] | None,
        existing_item: dict[str, Any] | None,
    ) -> None:
        """Evaluate a condition expression; raise ValidationError on failure.

        Supported format:
          {"function": "attribute_exists",     "attribute": "field"}
          {"function": "attribute_not_exists", "attribute": "field"}
          {"function": "attribute_equals",     "attribute": "field", "value": <v>}
        """
        if condition is None:
            return
        func = condition.get("function")
        if func not in _COND_FUNCS:
            raise ValidationError(
                f"unsupported condition function {func!r}; "
                f"supported: {sorted(_COND_FUNCS)}"
            )
        attr = condition.get("attribute")
        if not attr:
            raise ValidationError("condition missing 'attribute'")
        if func == "attribute_exists":
            if existing_item is None or attr not in existing_item:
                raise ValidationError(
                    f"condition failed: attribute_exists({attr!r}) — item does not exist "
                    f"or attribute is absent"
                )
        elif func == "attribute_not_exists":
            if existing_item is not None and attr in existing_item:
                raise ValidationError(
                    f"condition failed: attribute_not_exists({attr!r}) — attribute already present"
                )
        elif func == "attribute_equals":
            expected = condition.get("value")
            actual = existing_item.get(attr) if existing_item else None
            if actual != expected:
                raise ValidationError(
                    f"condition failed: attribute_equals({attr!r}, {expected!r}) — "
                    f"actual value is {actual!r}"
                )

    # --- items -------------------------------------------------------------
    def put_item(
        self,
        table: str,
        item: dict[str, Any],
        condition: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        row = self._require_table(table)
        if not isinstance(item, dict):
            raise ValidationError("item must be a dict")
        pk, sk = self._keys_for(row, item)
        if condition is not None:
            existing = self._get_item_raw(table, pk, sk)
            self._check_condition(condition, existing)
        self.storage.execute(
            """INSERT INTO ddb_items(table_name,pk,sk,item_json) VALUES (?,?,?,?)
               ON CONFLICT(table_name,pk,sk) DO UPDATE SET item_json=excluded.item_json""",
            (table, pk, sk, json.dumps(item)),
        )
        return dict(item)

    def get_item(self, table: str, key: dict[str, Any]) -> dict[str, Any] | None:
        row = self._require_table(table)
        pk, sk = self._keys_for(row, key)
        item = self._get_item_raw(table, pk, sk)
        if item is None:
            return None
        if self._is_expired(row, item):
            return None
        return item

    def _get_item_raw(
        self, table: str, pk: str, sk: str
    ) -> dict[str, Any] | None:
        found = self.storage.query_one(
            "SELECT item_json FROM ddb_items WHERE table_name=? AND pk=? AND sk=?",
            (table, pk, sk),
        )
        return json.loads(found["item_json"]) if found else None

    def delete_item(
        self,
        table: str,
        key: dict[str, Any],
        condition: dict[str, Any] | None = None,
    ) -> None:
        row = self._require_table(table)
        pk, sk = self._keys_for(row, key)
        if condition is not None:
            existing = self._get_item_raw(table, pk, sk)
            self._check_condition(condition, existing)
        self.storage.execute(
            "DELETE FROM ddb_items WHERE table_name=? AND pk=? AND sk=?", (table, pk, sk)
        )

    def update_item(
        self,
        table: str,
        key: dict[str, Any],
        update_expression: dict[str, Any],
        condition: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """Apply an UpdateExpression to an existing item.

        update_expression format::

            {
              "SET":    {"attr": value, ...},   # set attribute values
              "REMOVE": ["attr", ...],           # remove attributes
              "ADD":    {"attr": number, ...},   # add to numeric attribute
            }

        Keys are optional. Returns the updated item or None if no item existed.
        """
        row = self._require_table(table)
        pk, sk = self._keys_for(row, key)
        item = self._get_item_raw(table, pk, sk)
        if condition is not None:
            self._check_condition(condition, item)
        if item is None:
            # create a new item from the key + SET values only
            item = dict(key)
        set_map = update_expression.get("SET", {})
        remove_list = update_expression.get("REMOVE", [])
        add_map = update_expression.get("ADD", {})
        for attr, val in set_map.items():
            item[attr] = val
        for attr in remove_list:
            item.pop(attr, None)
        for attr, delta in add_map.items():
            current = item.get(attr, 0)
            try:
                item[attr] = float(current) + float(delta)
                # keep int if both are integral
                if item[attr] == int(item[attr]):
                    item[attr] = int(item[attr])
            except (TypeError, ValueError) as exc:
                raise ValidationError(
                    f"ADD requires numeric attribute; {attr!r} = {current!r}"
                ) from exc
        self.storage.execute(
            """INSERT INTO ddb_items(table_name,pk,sk,item_json) VALUES (?,?,?,?)
               ON CONFLICT(table_name,pk,sk) DO UPDATE SET item_json=excluded.item_json""",
            (table, pk, sk, json.dumps(item)),
        )
        return dict(item)

    def query(
        self,
        table: str,
        key_value: Any,
        sort_begins_with: str | None = None,
        sort_eq: Any | None = None,
        index_name: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return items with the given partition key value, ordered by sort key.

        Pass index_name to query a GSI or LSI instead of the base table.
        """
        table_row = self._require_table(table)
        if index_name:
            return self._query_index(table_row, table, index_name, key_value,
                                     sort_begins_with, sort_eq)
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
            item = json.loads(r["item_json"])
            if self._is_expired(table_row, item):
                continue
            out.append(item)
        return out

    def _query_index(
        self,
        table_row,
        table: str,
        index_name: str,
        key_value: Any,
        sort_begins_with: str | None,
        sort_eq: Any | None,
    ) -> list[dict[str, Any]]:
        """Query a GSI or LSI by scanning and filtering in Python.

        For a small local emulator this is acceptable; in production DynamoDB
        the service maintains projected indexes.
        """
        gsis = json.loads(table_row["gsi_json"] or "[]")
        lsis = json.loads(table_row["lsi_json"] or "[]")
        index_def = next(
            (i for i in gsis + lsis if i["name"] == index_name), None
        )
        if index_def is None:
            raise ValidationError(f"no such index: {index_name!r} on table {table!r}")
        idx_hk = index_def["hash_key"]
        idx_rk = index_def.get("range_key")
        # full scan then filter
        rows = self.storage.query(
            "SELECT item_json FROM ddb_items WHERE table_name=? ORDER BY pk,sk",
            (table,),
        )
        out = []
        for r in rows:
            item = json.loads(r["item_json"])
            if self._is_expired(table_row, item):
                continue
            if str(item.get(idx_hk, "")) != str(key_value):
                continue
            if idx_rk:
                sk_val = str(item.get(idx_rk, ""))
                if sort_eq is not None and sk_val != str(sort_eq):
                    continue
                if sort_begins_with is not None and not sk_val.startswith(sort_begins_with):
                    continue
            out.append(item)
        # sort by index sort key if present
        if idx_rk:
            out.sort(key=lambda x: str(x.get(idx_rk, "")))
        return out

    def scan(
        self,
        table: str,
        filters: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Full-table scan with optional attribute-equality filters."""
        table_row = self._require_table(table)
        rows = self.storage.query(
            "SELECT item_json FROM ddb_items WHERE table_name=? ORDER BY pk,sk", (table,)
        )
        items = []
        for r in rows:
            item = json.loads(r["item_json"])
            if self._is_expired(table_row, item):
                continue
            items.append(item)
        if filters:
            items = [it for it in items if all(it.get(k) == v for k, v in filters.items())]
        return items

    # --- batch operations --------------------------------------------------
    def batch_get_item(
        self,
        request_items: dict[str, list[dict[str, Any]]],
    ) -> dict[str, list[dict[str, Any]]]:
        """Fetch multiple items across one or more tables.

        request_items = {table_name: [key_dict, ...], ...}
        Returns {table_name: [item_or_none, ...], ...}
        """
        result: dict[str, list[dict[str, Any] | None]] = {}
        for table_name, keys in request_items.items():
            table_row = self._require_table(table_name)
            items_for_table = []
            for key in keys:
                pk, sk = self._keys_for(table_row, key)
                item = self._get_item_raw(table_name, pk, sk)
                if item is not None and self._is_expired(table_row, item):
                    item = None
                items_for_table.append(item)
            result[table_name] = items_for_table
        return result

    def batch_write_item(
        self,
        request_items: dict[str, list[dict[str, Any]]],
    ) -> dict[str, Any]:
        """Put or delete multiple items across one or more tables.

        Each request in the list is either:
          {"put": item_dict}  or  {"delete": key_dict}

        Returns {"unprocessed": {}} (all items are processed in this impl).
        """
        for table_name, requests in request_items.items():
            table_row = self._require_table(table_name)
            for req in requests:
                if "put" in req:
                    self.put_item(table_name, req["put"])
                elif "delete" in req:
                    pk, sk = self._keys_for(table_row, req["delete"])
                    self.storage.execute(
                        "DELETE FROM ddb_items WHERE table_name=? AND pk=? AND sk=?",
                        (table_name, pk, sk),
                    )
                else:
                    raise ValidationError(
                        "each batch_write_item request must have 'put' or 'delete'"
                    )
        return {"unprocessed": {}}

    # --- transact write ----------------------------------------------------
    def transact_write_items(
        self,
        transact_items: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """All-or-nothing multi-item write.

        Each item in transact_items is one of:
          {"put":    {"table": t, "item": i, "condition": c}}
          {"delete": {"table": t, "key": k,  "condition": c}}
          {"update": {"table": t, "key": k, "update_expression": ue, "condition": c}}

        Conditions are evaluated first; if any fails the entire transaction is
        rolled back (via exception) and no items are written.
        """
        if not transact_items:
            return {"transacted": 0}
        # Phase 1: validate all conditions before writing anything
        for op_wrap in transact_items:
            if "put" in op_wrap:
                op = op_wrap["put"]
                table_row = self._require_table(op["table"])
                item = op["item"]
                pk, sk = self._keys_for(table_row, item)
                existing = self._get_item_raw(op["table"], pk, sk)
                self._check_condition(op.get("condition"), existing)
            elif "delete" in op_wrap:
                op = op_wrap["delete"]
                table_row = self._require_table(op["table"])
                pk, sk = self._keys_for(table_row, op["key"])
                existing = self._get_item_raw(op["table"], pk, sk)
                self._check_condition(op.get("condition"), existing)
            elif "update" in op_wrap:
                op = op_wrap["update"]
                table_row = self._require_table(op["table"])
                pk, sk = self._keys_for(table_row, op["key"])
                existing = self._get_item_raw(op["table"], pk, sk)
                self._check_condition(op.get("condition"), existing)
            else:
                raise ValidationError(
                    "each transact_items entry must have 'put', 'delete', or 'update'"
                )
        # Phase 2: apply all operations
        count = 0
        for op_wrap in transact_items:
            if "put" in op_wrap:
                op = op_wrap["put"]
                self.put_item(op["table"], op["item"])
            elif "delete" in op_wrap:
                op = op_wrap["delete"]
                self.delete_item(op["table"], op["key"])
            elif "update" in op_wrap:
                op = op_wrap["update"]
                self.update_item(op["table"], op["key"], op["update_expression"])
            count += 1
        return {"transacted": count}
