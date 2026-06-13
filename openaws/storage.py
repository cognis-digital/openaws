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
                    created_at REAL NOT NULL,
                    versioning TEXT NOT NULL DEFAULT 'disabled'
                );
                CREATE TABLE IF NOT EXISTS s3_objects (
                    bucket TEXT NOT NULL,
                    key TEXT NOT NULL,
                    body BLOB NOT NULL,
                    content_type TEXT NOT NULL,
                    etag TEXT NOT NULL,
                    size INTEGER NOT NULL,
                    last_modified REAL NOT NULL,
                    meta_json TEXT NOT NULL DEFAULT '{}',
                    PRIMARY KEY (bucket, key)
                );
                CREATE TABLE IF NOT EXISTS s3_object_versions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bucket TEXT NOT NULL,
                    key TEXT NOT NULL,
                    version_id TEXT NOT NULL,
                    body BLOB NOT NULL,
                    content_type TEXT NOT NULL,
                    etag TEXT NOT NULL,
                    size INTEGER NOT NULL,
                    last_modified REAL NOT NULL,
                    is_latest INTEGER NOT NULL DEFAULT 0,
                    meta_json TEXT NOT NULL DEFAULT '{}'
                );
                CREATE TABLE IF NOT EXISTS s3_object_tags (
                    bucket TEXT NOT NULL,
                    key TEXT NOT NULL,
                    tag_key TEXT NOT NULL,
                    tag_value TEXT NOT NULL,
                    PRIMARY KEY (bucket, key, tag_key)
                );
                CREATE TABLE IF NOT EXISTS s3_multipart_uploads (
                    upload_id TEXT PRIMARY KEY,
                    bucket TEXT NOT NULL,
                    key TEXT NOT NULL,
                    content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
                    meta_json TEXT NOT NULL DEFAULT '{}',
                    created_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS s3_multipart_parts (
                    upload_id TEXT NOT NULL,
                    part_number INTEGER NOT NULL,
                    bucket TEXT NOT NULL,
                    key TEXT NOT NULL,
                    body BLOB NOT NULL,
                    etag TEXT NOT NULL,
                    size INTEGER NOT NULL,
                    PRIMARY KEY (upload_id, part_number)
                );
                CREATE TABLE IF NOT EXISTS ddb_tables (
                    name TEXT PRIMARY KEY,
                    hash_key TEXT NOT NULL,
                    range_key TEXT,
                    created_at REAL NOT NULL,
                    gsi_json TEXT NOT NULL DEFAULT '[]',
                    lsi_json TEXT NOT NULL DEFAULT '[]',
                    ttl_attribute TEXT
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
                    created_at REAL NOT NULL,
                    fifo INTEGER NOT NULL DEFAULT 0,
                    dedup_window REAL NOT NULL DEFAULT 300.0,
                    dlq_name TEXT,
                    max_receive_count INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS sqs_messages (
                    id TEXT PRIMARY KEY,
                    queue TEXT NOT NULL,
                    body TEXT NOT NULL,
                    receipt_handle TEXT,
                    visible_at REAL NOT NULL,
                    received_count INTEGER NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL,
                    group_id TEXT,
                    dedup_id TEXT,
                    attributes_json TEXT NOT NULL DEFAULT '{}'
                );
                CREATE TABLE IF NOT EXISTS lambda_functions (
                    name TEXT PRIMARY KEY,
                    source TEXT NOT NULL,
                    handler TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    env_json TEXT NOT NULL DEFAULT '{}',
                    description TEXT NOT NULL DEFAULT '',
                    timeout INTEGER NOT NULL DEFAULT 3
                );
                CREATE TABLE IF NOT EXISTS lambda_versions (
                    function_name TEXT NOT NULL,
                    version TEXT NOT NULL,
                    source TEXT NOT NULL,
                    handler TEXT NOT NULL,
                    env_json TEXT NOT NULL DEFAULT '{}',
                    description TEXT NOT NULL DEFAULT '',
                    created_at REAL NOT NULL,
                    PRIMARY KEY (function_name, version)
                );
                CREATE TABLE IF NOT EXISTS lambda_aliases (
                    function_name TEXT NOT NULL,
                    alias TEXT NOT NULL,
                    version TEXT NOT NULL,
                    description TEXT NOT NULL DEFAULT '',
                    created_at REAL NOT NULL,
                    PRIMARY KEY (function_name, alias)
                );
                CREATE TABLE IF NOT EXISTS lambda_layers (
                    name TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    description TEXT NOT NULL DEFAULT '',
                    compatible_runtimes_json TEXT NOT NULL DEFAULT '[]',
                    created_at REAL NOT NULL,
                    PRIMARY KEY (name, version)
                );
                CREATE TABLE IF NOT EXISTS lambda_async_queue (
                    id TEXT PRIMARY KEY,
                    function_name TEXT NOT NULL,
                    event_json TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'QUEUED',
                    created_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS kinesis_streams (
                    name TEXT PRIMARY KEY,
                    shard_count INTEGER NOT NULL DEFAULT 1,
                    status TEXT NOT NULL DEFAULT 'ACTIVE',
                    created_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS kinesis_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    stream TEXT NOT NULL,
                    shard_id INTEGER NOT NULL,
                    seq INTEGER NOT NULL,
                    partition_key TEXT NOT NULL,
                    data_b64 TEXT NOT NULL,
                    arrival_ts REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_kinesis_records_stream_shard_seq
                    ON kinesis_records(stream, shard_id, seq);

                -- SNS
                CREATE TABLE IF NOT EXISTS sns_topics (
                    name TEXT PRIMARY KEY,
                    arn TEXT NOT NULL,
                    created_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS sns_subscriptions (
                    id TEXT PRIMARY KEY,
                    arn TEXT NOT NULL,
                    topic TEXT NOT NULL,
                    protocol TEXT NOT NULL,
                    endpoint TEXT NOT NULL,
                    created_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS sns_deliveries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    msg_id TEXT NOT NULL,
                    topic TEXT NOT NULL,
                    endpoint TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    ts REAL NOT NULL
                );

                -- EventBridge
                CREATE TABLE IF NOT EXISTS eb_buses (
                    name TEXT PRIMARY KEY,
                    arn TEXT NOT NULL,
                    created_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS eb_rules (
                    name TEXT NOT NULL,
                    bus TEXT NOT NULL DEFAULT 'default',
                    arn TEXT NOT NULL,
                    pattern_json TEXT,
                    schedule TEXT,
                    state TEXT NOT NULL DEFAULT 'ENABLED',
                    created_at REAL NOT NULL,
                    PRIMARY KEY (name, bus)
                );
                CREATE TABLE IF NOT EXISTS eb_targets (
                    rule TEXT NOT NULL,
                    bus TEXT NOT NULL DEFAULT 'default',
                    id TEXT NOT NULL,
                    type TEXT NOT NULL,
                    arn TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    PRIMARY KEY (rule, bus, id)
                );

                -- Step Functions
                CREATE TABLE IF NOT EXISTS sf_state_machines (
                    name TEXT PRIMARY KEY,
                    arn TEXT NOT NULL,
                    definition_json TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'ACTIVE',
                    created_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS sf_executions (
                    exec_id TEXT PRIMARY KEY,
                    arn TEXT NOT NULL,
                    state_machine TEXT NOT NULL,
                    status TEXT NOT NULL,
                    input_json TEXT,
                    output_json TEXT,
                    error TEXT,
                    cause TEXT,
                    started_at REAL NOT NULL,
                    stopped_at REAL
                );

                -- API Gateway
                CREATE TABLE IF NOT EXISTS apigw_apis (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT NOT NULL DEFAULT '',
                    created_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS apigw_resources (
                    id TEXT PRIMARY KEY,
                    api_id TEXT NOT NULL,
                    path TEXT NOT NULL,
                    http_method TEXT NOT NULL,
                    integration_type TEXT NOT NULL DEFAULT 'lambda',
                    integration_uri TEXT NOT NULL DEFAULT '',
                    created_at REAL NOT NULL
                );

                -- SES
                CREATE TABLE IF NOT EXISTS ses_identities (
                    email TEXT PRIMARY KEY,
                    verified_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS ses_emails (
                    msg_id TEXT PRIMARY KEY,
                    payload_json TEXT NOT NULL,
                    sent_at REAL NOT NULL
                );

                -- IAM
                CREATE TABLE IF NOT EXISTS iam_users (
                    username TEXT PRIMARY KEY,
                    path TEXT NOT NULL DEFAULT '/',
                    arn TEXT NOT NULL,
                    created_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS iam_access_keys (
                    key_id TEXT PRIMARY KEY,
                    secret_access_key TEXT NOT NULL,
                    username TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'Active',
                    created_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS iam_groups (
                    group_name TEXT PRIMARY KEY,
                    path TEXT NOT NULL DEFAULT '/',
                    arn TEXT NOT NULL,
                    created_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS iam_user_group_memberships (
                    username TEXT NOT NULL,
                    group_name TEXT NOT NULL,
                    PRIMARY KEY (username, group_name)
                );
                CREATE TABLE IF NOT EXISTS iam_roles (
                    role_name TEXT PRIMARY KEY,
                    path TEXT NOT NULL DEFAULT '/',
                    arn TEXT NOT NULL,
                    assume_role_policy_json TEXT NOT NULL DEFAULT '{}',
                    description TEXT NOT NULL DEFAULT '',
                    created_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS iam_policies (
                    policy_name TEXT PRIMARY KEY,
                    arn TEXT NOT NULL,
                    path TEXT NOT NULL DEFAULT '/',
                    description TEXT NOT NULL DEFAULT '',
                    document_json TEXT NOT NULL DEFAULT '{}',
                    created_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS iam_inline_policies (
                    principal_type TEXT NOT NULL,
                    principal_name TEXT NOT NULL,
                    policy_name TEXT NOT NULL,
                    document_json TEXT NOT NULL DEFAULT '{}',
                    PRIMARY KEY (principal_type, principal_name, policy_name)
                );
                CREATE TABLE IF NOT EXISTS iam_attachments (
                    principal_type TEXT NOT NULL,
                    principal_name TEXT NOT NULL,
                    policy_name TEXT NOT NULL,
                    PRIMARY KEY (principal_type, principal_name, policy_name)
                );

                -- STS
                CREATE TABLE IF NOT EXISTS sts_sessions (
                    session_id TEXT PRIMARY KEY,
                    access_key_id TEXT NOT NULL,
                    secret_key TEXT NOT NULL,
                    session_token TEXT NOT NULL,
                    assumed_role_arn TEXT,
                    session_name TEXT NOT NULL DEFAULT '',
                    issued_at REAL NOT NULL,
                    expires_at REAL NOT NULL
                );

                -- KMS
                CREATE TABLE IF NOT EXISTS kms_keys (
                    key_id TEXT PRIMARY KEY,
                    arn TEXT NOT NULL,
                    description TEXT NOT NULL DEFAULT '',
                    key_usage TEXT NOT NULL DEFAULT 'ENCRYPT_DECRYPT',
                    key_spec TEXT NOT NULL DEFAULT 'SYMMETRIC_DEFAULT',
                    key_material_b64 TEXT NOT NULL,
                    state TEXT NOT NULL DEFAULT 'Enabled',
                    rotation_enabled INTEGER NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS kms_aliases (
                    alias_name TEXT PRIMARY KEY,
                    target_key_id TEXT NOT NULL,
                    created_at REAL NOT NULL
                );

                -- Secrets Manager
                CREATE TABLE IF NOT EXISTS sm_secrets (
                    name TEXT PRIMARY KEY,
                    arn TEXT NOT NULL,
                    description TEXT NOT NULL DEFAULT '',
                    rotation_enabled INTEGER NOT NULL DEFAULT 0,
                    rotation_lambda_arn TEXT,
                    rotation_rules_json TEXT,
                    deleted_at REAL,
                    created_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS sm_versions (
                    version_id TEXT PRIMARY KEY,
                    secret_name TEXT NOT NULL,
                    secret_string TEXT,
                    secret_binary_b64 TEXT,
                    stages_json TEXT NOT NULL DEFAULT '["AWSCURRENT"]',
                    created_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS sm_tags (
                    secret_name TEXT NOT NULL,
                    tag_key TEXT NOT NULL,
                    tag_value TEXT NOT NULL,
                    PRIMARY KEY (secret_name, tag_key)
                );

                -- SSM Parameter Store
                CREATE TABLE IF NOT EXISTS ssm_parameters (
                    name TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    type TEXT NOT NULL DEFAULT 'String',
                    description TEXT NOT NULL DEFAULT '',
                    kms_key_id TEXT,
                    version INTEGER NOT NULL DEFAULT 1,
                    created_at REAL NOT NULL,
                    last_modified_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS ssm_history (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    value TEXT NOT NULL,
                    type TEXT NOT NULL DEFAULT 'String',
                    version INTEGER NOT NULL,
                    created_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS ssm_tags (
                    param_name TEXT NOT NULL,
                    tag_key TEXT NOT NULL,
                    tag_value TEXT NOT NULL,
                    PRIMARY KEY (param_name, tag_key)
                );

                -- CloudWatch
                CREATE TABLE IF NOT EXISTS cw_metrics (
                    id TEXT PRIMARY KEY,
                    namespace TEXT NOT NULL,
                    metric_name TEXT NOT NULL,
                    value REAL NOT NULL,
                    unit TEXT NOT NULL DEFAULT 'None',
                    dimensions_json TEXT NOT NULL DEFAULT '[]',
                    ts REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_cw_metrics_ns_name_ts
                    ON cw_metrics(namespace, metric_name, ts);
                CREATE TABLE IF NOT EXISTS cw_log_groups (
                    log_group_name TEXT PRIMARY KEY,
                    kms_key_id TEXT,
                    created_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS cw_log_streams (
                    log_group_name TEXT NOT NULL,
                    log_stream_name TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    PRIMARY KEY (log_group_name, log_stream_name)
                );
                CREATE TABLE IF NOT EXISTS cw_log_events (
                    id TEXT PRIMARY KEY,
                    log_group_name TEXT NOT NULL,
                    log_stream_name TEXT NOT NULL,
                    ts REAL NOT NULL,
                    message TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_cw_log_events_group_stream_ts
                    ON cw_log_events(log_group_name, log_stream_name, ts);
                CREATE TABLE IF NOT EXISTS cw_alarms (
                    alarm_name TEXT PRIMARY KEY,
                    namespace TEXT NOT NULL,
                    metric_name TEXT NOT NULL,
                    comparison_operator TEXT NOT NULL,
                    threshold REAL NOT NULL,
                    evaluation_periods INTEGER NOT NULL DEFAULT 1,
                    period INTEGER NOT NULL DEFAULT 60,
                    statistic TEXT NOT NULL DEFAULT 'Average',
                    description TEXT NOT NULL DEFAULT '',
                    alarm_actions_json TEXT NOT NULL DEFAULT '[]',
                    ok_actions_json TEXT NOT NULL DEFAULT '[]',
                    insufficient_data_actions_json TEXT NOT NULL DEFAULT '[]',
                    treat_missing_data TEXT NOT NULL DEFAULT 'missing',
                    state TEXT NOT NULL DEFAULT 'INSUFFICIENT_DATA',
                    state_updated_at REAL NOT NULL,
                    created_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS cw_alarm_history (
                    id TEXT PRIMARY KEY,
                    alarm_name TEXT NOT NULL,
                    state TEXT NOT NULL,
                    reason TEXT NOT NULL DEFAULT '',
                    ts REAL NOT NULL
                );

                -- Cognito
                CREATE TABLE IF NOT EXISTS cognito_user_pools (
                    pool_id TEXT PRIMARY KEY,
                    pool_name TEXT NOT NULL,
                    arn TEXT NOT NULL,
                    signing_secret TEXT NOT NULL,
                    password_policy_json TEXT NOT NULL DEFAULT '{}',
                    auto_verified_attrs_json TEXT NOT NULL DEFAULT '[]',
                    username_attrs_json TEXT NOT NULL DEFAULT '[]',
                    created_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS cognito_pool_clients (
                    client_id TEXT PRIMARY KEY,
                    pool_id TEXT NOT NULL,
                    client_name TEXT NOT NULL,
                    client_secret TEXT,
                    explicit_auth_flows_json TEXT NOT NULL DEFAULT '[]',
                    created_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS cognito_users (
                    user_id TEXT PRIMARY KEY,
                    pool_id TEXT NOT NULL,
                    username TEXT NOT NULL,
                    password_hash TEXT NOT NULL,
                    password_salt TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'UNCONFIRMED',
                    attributes_json TEXT NOT NULL DEFAULT '[]',
                    confirmation_code TEXT,
                    created_at REAL NOT NULL,
                    UNIQUE (pool_id, username)
                );
                CREATE TABLE IF NOT EXISTS cognito_tokens (
                    refresh_token TEXT PRIMARY KEY,
                    pool_id TEXT NOT NULL,
                    username TEXT NOT NULL,
                    client_id TEXT NOT NULL,
                    issued_at REAL NOT NULL,
                    expires_at REAL NOT NULL
                );
                """
            )
            self._conn.commit()
            # Migrate existing file-backed databases that were created before
            # the new columns were added.  ALTER TABLE IF NOT EXISTS ... ADD COLUMN
            # is not supported in older SQLite; we catch OperationalError gracefully.
            self._migrate_columns()

    # ------------------------------------------------------------------
    # Migration helpers for file-backed databases
    # ------------------------------------------------------------------

    _MIGRATIONS: list[tuple[str, str]] = [
        # (table, column_definition)
        ("sqs_queues", "fifo INTEGER NOT NULL DEFAULT 0"),
        ("sqs_queues", "dedup_window REAL NOT NULL DEFAULT 300.0"),
        ("sqs_queues", "dlq_name TEXT"),
        ("sqs_queues", "max_receive_count INTEGER NOT NULL DEFAULT 0"),
        ("sqs_messages", "group_id TEXT"),
        ("sqs_messages", "dedup_id TEXT"),
        ("sqs_messages", "attributes_json TEXT NOT NULL DEFAULT '{}'"),
        ("lambda_functions", "env_json TEXT NOT NULL DEFAULT '{}'"),
        ("lambda_functions", "description TEXT NOT NULL DEFAULT ''"),
        ("lambda_functions", "timeout INTEGER NOT NULL DEFAULT 3"),
    ]

    def _migrate_columns(self) -> None:
        """Idempotently add new columns to pre-existing tables."""
        for table, col_def in self._MIGRATIONS:
            col_name = col_def.split()[0]
            try:
                existing = [
                    row[1]
                    for row in self._conn.execute(f"PRAGMA table_info({table})").fetchall()
                ]
                if col_name not in existing:
                    self._conn.execute(
                        f"ALTER TABLE {table} ADD COLUMN {col_def}"
                    )
                    self._conn.commit()
            except Exception:  # noqa: BLE001 - defensive; table may not exist yet
                pass

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------

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
