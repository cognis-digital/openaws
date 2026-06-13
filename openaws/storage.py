"""Shared storage backend for openaws services.

A thin wrapper around sqlite3 that provides a single connection (in-memory or
file-backed) shared by all services. Each service owns its own tables; this
module just manages the connection, schema bootstrap, and thread safety.

Using ``":memory:"`` (the default) gives a fast, ephemeral store used by the
test suite. Passing a directory path persists data to ``<dir>/openaws.db``.
"""

from __future__ import annotations

import os
import sqlite3
import threading


class Storage:
    """A thread-safe sqlite-backed store shared by every service."""

    def __init__(self, data_dir: str | None = None):
        self.data_dir = data_dir
        self._lock = threading.RLock()
        if data_dir in (None, ":memory:"):
            self.path = ":memory:"
            # check_same_thread=False so the in-memory DB is usable from the
            # HTTP server's worker threads; we serialize access with _lock.
            self._conn = sqlite3.connect(":memory:", check_same_thread=False)
        else:
            os.makedirs(data_dir, exist_ok=True)
            self.path = os.path.join(data_dir, "openaws.db")
            self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._bootstrap()

    def _bootstrap(self) -> None:
        with self._lock:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS s3_buckets (
                    name TEXT PRIMARY KEY,
                    created_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS s3_objects (
                    bucket TEXT NOT NULL,
                    key TEXT NOT NULL,
                    body BLOB NOT NULL,
                    content_type TEXT NOT NULL,
                    etag TEXT NOT NULL,
                    size INTEGER NOT NULL,
                    last_modified REAL NOT NULL,
                    PRIMARY KEY (bucket, key)
                );
                CREATE TABLE IF NOT EXISTS ddb_tables (
                    name TEXT PRIMARY KEY,
                    hash_key TEXT NOT NULL,
                    range_key TEXT,
                    created_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS ddb_items (
                    table_name TEXT NOT NULL,
                    pk TEXT NOT NULL,
                    sk TEXT NOT NULL,
                    item_json TEXT NOT NULL,
                    PRIMARY KEY (table_name, pk, sk)
                );
                CREATE TABLE IF NOT EXISTS sqs_queues (
                    name TEXT PRIMARY KEY,
                    visibility_timeout REAL NOT NULL,
                    created_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS sqs_messages (
                    id TEXT PRIMARY KEY,
                    queue TEXT NOT NULL,
                    body TEXT NOT NULL,
                    receipt_handle TEXT,
                    visible_at REAL NOT NULL,
                    received_count INTEGER NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS lambda_functions (
                    name TEXT PRIMARY KEY,
                    source TEXT NOT NULL,
                    handler TEXT NOT NULL,
                    created_at REAL NOT NULL
                );
                """
            )
            self._conn.commit()

    def execute(self, sql: str, params: tuple = ()):  # pragma: no cover - thin
        with self._lock:
            cur = self._conn.execute(sql, params)
            self._conn.commit()
            return cur

    def query(self, sql: str, params: tuple = ()):
        with self._lock:
            cur = self._conn.execute(sql, params)
            return cur.fetchall()

    def query_one(self, sql: str, params: tuple = ()):
        with self._lock:
            cur = self._conn.execute(sql, params)
            return cur.fetchone()

    def close(self) -> None:
        with self._lock:
            self._conn.close()
