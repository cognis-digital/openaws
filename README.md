# openaws

## Usage — step by step

`openaws` runs local AWS-style services (S3 / DynamoDB / SQS / Lambda / Kinesis / SNS /
EventBridge / Step Functions / API Gateway / SES / IAM / STS / KMS / Secrets Manager /
SSM / CloudWatch / Cognito) for development, plus convenience subcommands for S3 and SQS.

1. **Install** (editable from a clone, or from the wheel):
   ```bash
   pip install -e .
   # provides the `openaws` console script
   ```
2. **Start the local server** (defaults to `127.0.0.1:4566`; pass `--data-dir`
   on the top-level command to persist instead of in-memory):
   ```bash
   openaws --data-dir ./aws-data serve --host 127.0.0.1 --port 4566
   # services: /s3  /dynamodb  /sqs  /lambda  /kinesis  /sns  /eventbridge  /stepfunctions
   #           /apigateway  /ses  /iam  /sts  /kms  /secretsmanager  /ssm  /cloudwatch  /cognito
   ```
3. **Drive S3 from the CLI** — make a bucket, put/get objects, list:
   ```bash
   openaws --data-dir ./aws-data s3 mb my-bucket
   openaws --data-dir ./aws-data s3 put my-bucket key.txt ./local.txt
   openaws --data-dir ./aws-data s3 ls my-bucket
   ```
4. **Use the output / SQS flow.** `s3 get` writes the object bytes to stdout;
   the SQS subcommands return ids and message bodies you can pipe onward:
   ```bash
   openaws --data-dir ./aws-data sqs create jobs
   openaws --data-dir ./aws-data sqs send jobs '{"task":"resize"}'
   openaws --data-dir ./aws-data sqs receive jobs --max 5
   ```
5. **Point your AWS SDK at it in CI.** Run the server as a background service
   and set the endpoint URL so existing SDK code hits openaws:
   ```bash
   openaws serve --port 4566 &
   aws --endpoint-url http://127.0.0.1:4566 s3 ls    # `openaws version` prints the version
   ```

## What is this?

**openaws** is an independent, open-source tool that runs a set of **AWS-style
cloud services on your own machine** — entirely offline. You start one local server
and get an S3-style object store, a DynamoDB-style table store, an SQS-style message
queue (with FIFO, DLQs and message attributes), a Lambda-style function runner (with
env vars, versions, aliases, async invocation and layer metadata), a Kinesis Data
Streams emulator, an SNS pub/sub bus, an EventBridge event router, a Step Functions
synchronous executor, an API Gateway REST router, an SES email-capture sink, a full
IAM service (users, groups, roles, managed+inline policies, attach/detach, simulate),
an STS token service (AssumeRole, GetCallerIdentity, session tokens), a KMS key
management service (CMKs, encrypt/decrypt, data keys, aliases, rotation), a Secrets
Manager (secrets with versioning and rotation stubs), an SSM Parameter Store
(String/SecureString/StringList, hierarchy, history), a CloudWatch service (metrics,
log groups/streams/events, alarms), and a Cognito user pools service (sign-up,
confirm, JWT-style tokens) — all backed by a local SQLite file (or in-memory for tests).

It exists so that **developers can build and test cloud-shaped applications without a
cloud account, without network access, and without spending money**. Instead of
pointing your code at real cloud endpoints during development, point it at openaws on
`localhost`. It is in the same spirit as LocalStack, MinIO, and the Firebase Emulator
Suite: a fast, disposable, local stand-in for cloud primitives.

Who it's for:

- Developers who want to iterate on storage / queue / serverless logic offline.
- CI pipelines that need deterministic, dependency-free service doubles.
- Anyone learning how object stores, key/value tables, queues, and function
  event-sources fit together, with a tiny readable codebase to study.

> **DISCLAIMER.** openaws is an *independent open reimplementation* intended for
> **LOCAL development and testing only**. It is **NOT affiliated with, endorsed by, or
> sponsored by** Amazon Web Services, Inc. or any vendor. Vendor and service names
> (S3, DynamoDB, SQS, Lambda, Kinesis, SNS, EventBridge, Step Functions, API Gateway,
> SES, AWS) are used **only nominatively** to describe API compatibility. openaws
> implements a **compatible SUBSET** of each service's behaviour and is **NOT for
> production use**.

## Architecture

openaws is pure Python standard library — no third-party runtime dependencies.

```
openaws/
  storage.py          shared SQLite backend (in-memory or file-backed), thread-safe
  s3.py               S3Service             — buckets, objects, multipart, versioning, tagging, copy, presigned-URL tokens
  dynamodb.py         DynamoDBService       — tables, items, GSI/LSI, conditional writes, batch ops, transactions, TTL, UpdateExpression
  sqs.py              SQSService            — queues & messages (visibility timeout, FIFO, DLQ redrive, message attributes)
  lambdas.py          LambdaService         — register & invoke Python handlers + env vars + versions/aliases + async + layers metadata
  kinesis.py          KinesisService        — streams, shards, put/get records with sequence numbers
  sns.py              SNSService            — topics, subscriptions, publish with fan-out to SQS/Lambda/log
  eventbridge.py      EventBridgeService    — event buses, rules (pattern match), targets to Lambda/SQS
  stepfunctions.py    StepFunctionsService  — state machine definition + synchronous execution (Task/Choice/Pass/Wait/Parallel/Succeed/Fail)
  apigateway.py       APIGatewayService     — REST APIs, routes (Lambda/mock integration), path-param routing, invocation
  ses.py              SESService            — email capture (send_email stored locally; list/get by recipient)
  iam.py              IAMService            — users, groups, roles, managed+inline policies, attach/detach, simulate_principal_policy
  sts.py              STSService            — AssumeRole, GetCallerIdentity, GetSessionToken, session revoke
  kms.py              KMSService            — CMKs, encrypt/decrypt, GenerateDataKey, aliases, key rotation
  secretsmanager.py   SecretsManagerService — secrets, versions (AWSCURRENT/AWSPREVIOUS), rotation stub, tags
  ssm.py              SSMService            — Parameter Store (String/StringList/SecureString + KMS), hierarchy, history
  cloudwatch.py       CloudWatchService     — metrics put/get/list, log groups/streams/events, alarms + history
  cognito.py          CognitoService        — user pools, clients, sign-up/confirm/sign-in, JWT-style tokens, refresh, sign-out
  errors.py           shared HTTP-mapped error types
  server.py           one ThreadingHTTPServer exposing every service under path prefixes
  cli.py              `openaws` console entry point (serve + convenience subcommands)
  __main__.py         `python -m openaws`
```

All services share a single `Storage` instance, so a Lambda function can read an
SQS message, write to a DynamoDB table, fan out via SNS, trigger an EventBridge
rule, or be called from an API Gateway route — exactly the kind of cross-service
flow you want to test locally.

The HTTP server routes by path prefix. S3 is RESTful (binary bodies); the other
services use a tiny JSON "action" protocol (`POST {"action": "...", ...}`). API
Gateway invocation uses a second path prefix `/apigw/<api_id>/<path>`.

| Path | Service | Protocol |
| --- | --- | --- |
| `/s3/...` | S3 object store | REST (`PUT`/`GET`/`DELETE`/`GET` list) |
| `/dynamodb` | DynamoDB table store | JSON action |
| `/sqs` | SQS queue | JSON action |
| `/lambda` | Lambda runner | JSON action |
| `/kinesis` | Kinesis Data Streams | JSON action |
| `/sns` | SNS pub/sub | JSON action |
| `/eventbridge` | EventBridge buses/rules/targets | JSON action |
| `/stepfunctions` | Step Functions state machines | JSON action |
| `/apigateway` | API Gateway management | JSON action |
| `/apigw/<api_id>/...` | API Gateway invocation | REST proxy |
| `/ses` | SES email capture | JSON action |
| `/iam` | IAM users/groups/roles/policies | JSON action |
| `/sts` | STS token service | JSON action |
| `/kms` | KMS key management | JSON action |
| `/secretsmanager` | Secrets Manager | JSON action |
| `/ssm` | SSM Parameter Store | JSON action |
| `/cloudwatch` | CloudWatch metrics/logs/alarms | JSON action |
| `/cognito` | Cognito user pools | JSON action |
| `/` `/health` | health & service listing | `GET` |

## Services

| Service | Class | Implemented | Roadmap (not implemented) |
| --- | --- | --- | --- |
| **S3** | `S3Service` | create/list/delete bucket; put/get/list(+prefix+delimiter)/delete object; MD5 ETags; multipart upload (create/upload-part/complete/abort/list-parts); object versioning (enable/suspend, per-version get/delete, list-versions); object tagging (put/get/delete); object copy (cross-bucket); per-object metadata; presigned-URL HMAC token (generate + verify); prefix+delimiter common-prefixes listing | ACLs, bucket policies, website hosting, lifecycle rules, replication |
| **DynamoDB** | `DynamoDBService` | create/describe/list/delete table; put/get/delete/update item; query (PK + sort `begins_with`/`eq`); scan (+ equality filters); Global Secondary Indexes (GSI); Local Secondary Indexes (LSI); query by index; conditional writes (`attribute_exists`/`attribute_not_exists`/`attribute_equals`); `BatchGetItem`; `BatchWriteItem`; `TransactWriteItems` (all-or-nothing with per-op conditions); TTL (per-table attribute, expired items filtered); `UpdateExpression` subset (SET/REMOVE/ADD) | full expression language, streams, on-demand billing simulation, parallel scan |
| **SQS** | `SQSService` | create/list/delete queue; send/receive/delete message; per-queue visibility timeout + redelivery; receive-count; **FIFO queues** (`.fifo` suffix, `message_group_id`, exactly-once deduplication within configurable window); **DLQ redrive** (configurable `max_receive_count`; exceeded messages moved to dead-letter queue); **message attributes** (per-message key→{data_type,string_value}); `get_queue_attributes` | long polling, batch operations, message timers |
| **Lambda** | `LambdaService` | register callable or source+handler; synchronous invoke; **environment variables** (per-function `env_vars` dict, injected into context); **function configuration updates** (env/description/timeout); **versions** (`publish_version`, `list_versions`); **aliases** (create/update/list/delete alias → version pointer); **async invocation** (`invoke_async` queues; `process_async_queue` executes; `list_async_invocations`); **layers metadata** (`add_layer_version`, `list_layer_versions`, `list_layers`); SQS event-source (poll→invoke→delete); S3 ObjectCreated event | concurrency limits, actual layer code loading, destinations |
| **Kinesis Data Streams** | `KinesisService` | create/delete/describe/list stream; configurable shard count; `PutRecord` (bytes or base64, CRC32 shard assignment); `PutRecords` (batch); `GetShardIterator` (TRIM_HORIZON / AT_SEQUENCE_NUMBER / AFTER_SEQUENCE_NUMBER / LATEST); `GetRecords` (paged, with `NextShardIterator`); monotonic sequence numbers; `DescribeStream` shard metadata | enhanced fan-out, server-side encryption, record retention policies, resharding |
| **SNS** | `SNSService` | create/list/delete topic; `subscribe` (protocols: `sqs`, `lambda`, `log`); `list_subscriptions` (global or per-topic); `unsubscribe`; `publish` with fan-out (SQS delivery, Lambda delivery, log-capture for tests); message attributes; `get_deliveries` (log-protocol captures) | email/HTTP/HTTPS delivery, filter policies, FIFO topics, message archival |
| **EventBridge** | `EventBridgeService` | create/list/delete event bus; `put_rule` (event pattern or schedule expression, upsert); `list_rules`; `delete_rule`; `put_targets` / `list_targets` / `remove_targets` (types: `lambda`, `sqs`); `put_events` with pattern-matched routing; pattern syntax: source/detail-type/detail field equality, prefix, anything-but, numeric comparisons, AND/OR/NOT, exists | scheduled invocation (cron), cross-account buses, replay, SaaS integrations |
| **Step Functions** | `StepFunctionsService` | create/describe/list/delete state machine (ASL `definition`); `start_execution` (synchronous, returns final output); `list_executions`; `describe_execution`; state types: **Task** (Lambda invoke via Resource), **Pass** (inject result, ResultPath), **Choice** (StringEquals/NotEquals/LessThan/GreaterThan, NumericEquals/NotEquals/LessThan/GreaterThan/LessThanEquals/GreaterThanEquals, BooleanEquals, IsNull/IsPresent/IsString/IsNumeric/IsBoolean, AND/OR/NOT, Default), **Wait** (Seconds, fast_wait flag for tests), **Parallel** (concurrent branches, results list), **Succeed**, **Fail** | async execution, activities, `waitForTaskToken`, retry/catch, Map state, Express workflows |
| **API Gateway** | `APIGatewayService` | create/list/delete REST API; `create_resource` (upsert route: `http_method` + `path` → integration); `list_resources`; `delete_resource`; integration types: `lambda` (proxy event + response passthrough), `mock` (200 stub); path-parameter routing (`/users/{id}`); invocation via `invoke()` or `/apigw/<api_id>/<path>` HTTP proxy | authorizers, stages/deployments, usage plans, WebSocket APIs, request validators |
| **SES** | `SESService` | `verify_email_identity` / `list_identities` / `delete_identity`; `send_email` (source, to/cc/bcc/reply-to, subject, body_text/body_html — captured in SQLite, never sent); `list_emails` (filter by `to_address`, `limit`); `get_email` by message-id; `delete_emails` (purge for test isolation) | bounce/complaint simulation, configuration sets, templates, bulk send, DKIM/DMARC |
| **IAM** | `IAMService` | `create_user` / `get_user` / `delete_user` / `list_users`; **access keys** (`create_access_key`, `list_access_keys`, `delete_access_key`); **groups** (`create_group`, `delete_group`, `list_groups`, `add_user_to_group`, `remove_user_from_group`, `list_groups_for_user`); **roles** (`create_role`, `get_role`, `delete_role`, `list_roles`); **managed policies** (`create_policy`, `get_policy`, `delete_policy`, `list_policies`); **inline policies** (`put_inline_policy`, `get_inline_policy`, `delete_inline_policy`, `list_inline_policies`) for users/groups/roles; **attach/detach** managed policies to users/groups/roles; `simulate_principal_policy` (Allow/explicitDeny/implicitDeny evaluator, wildcard action matching) | password policies, MFA, permission boundaries, service-linked roles, CloudTrail integration |
| **STS** | `STSService` | `assume_role` (issues temporary ASIA… credentials + session token, stored in `sts_sessions`); `get_session_token` (same credential shape); `get_caller_identity` (resolves access key → user ARN or assumed-role ARN); `list_sessions`; `revoke_session` | cross-account trust, ExternalId enforcement, token broker integration |
| **KMS** | `KMSService` | `create_key` (generates 32-byte key material, stores as base64); `describe_key` / `list_keys` / `schedule_key_deletion` / `disable_key` / `enable_key`; `encrypt` / `decrypt` (HMAC-SHA256 stream cipher with IV + auth tag; optional encryption context mixed via HMAC); `generate_data_key` (random plaintext + encrypted copy); `generate_data_key_without_plaintext`; **aliases** (`create_alias`, `list_aliases`, `delete_alias`); **key rotation** (`enable_key_rotation`, `disable_key_rotation`, `get_key_rotation_status`) | HSM-backed key material, cross-region replication, CloudHSM integration, FIPS endpoints |
| **Secrets Manager** | `SecretsManagerService` | `create_secret` / `describe_secret` / `list_secrets` / `update_secret` / `delete_secret` (soft + force) / `restore_secret`; `put_secret_value` (new version, demotes AWSCURRENT → AWSPREVIOUS); `get_secret_value` (by stage or version-id); `list_secret_version_ids`; `rotate_secret` (stores rotation Lambda ARN + rules — stub only); `tag_resource` / `untag_resource` / `list_tags_for_resource` | actual rotation Lambda invocation, cross-account replication, automatic secret generation |
| **SSM Parameter Store** | `SSMService` | `put_parameter` (String / StringList / SecureString with KMS encrypt); `get_parameter` / `get_parameters` (by name list, with_decryption); `get_parameters_by_path` (hierarchy prefix, recursive); `delete_parameter` / `delete_parameters` (batch); `describe_parameters` (with key/option/value filters); `list_parameter_history` (full version trail); tags (`list_tags_for_resource`) | Advanced Tier (8KB limits), expiration policies, change notifications |
| **CloudWatch** | `CloudWatchService` | **Metrics**: `put_metric_data` (namespace + data points with timestamp + dimensions); `get_metric_statistics` (period buckets, Average/Sum/Maximum/Minimum/SampleCount); `list_metrics`; **Logs**: `create_log_group` / `delete_log_group` / `list_log_groups` (prefix filter); `create_log_stream` / `delete_log_stream` / `list_log_streams`; `put_log_events` / `get_log_events` (time range, limit); `filter_log_events` (substring pattern, multi-stream); **Alarms**: `put_metric_alarm` (threshold + comparison operator, upsert); `describe_alarms` (filter by name or state); `describe_alarm`; `delete_alarm`; `set_alarm_state` (manual override); `get_alarm_history` | metric math, composite alarms, anomaly detection, live tail, CloudWatch Logs Insights, EMF |
| **Cognito** | `CognitoService` | `create_user_pool` / `describe_user_pool` / `delete_user_pool` / `list_user_pools`; **pool clients** (`create_user_pool_client`, `describe_user_pool_client`, `list_user_pool_clients`, `delete_user_pool_client`, `generate_secret`); `sign_up` (SHA-256 hashed password + confirmation code); `confirm_sign_up` / `admin_confirm_sign_up`; `initiate_auth` (USER_PASSWORD_AUTH, REFRESH_TOKEN_AUTH); **admin ops** (`admin_create_user`, `admin_delete_user`, `admin_set_user_password`, `admin_get_user`); `get_user` (from access token); `list_users`; **JWT-style tokens** (HMAC-SHA256 signed header.payload.signature); `global_sign_out` (revoke refresh tokens); `forgot_password` / `confirm_forgot_password` (reset-code stub) | Lambda triggers, identity pools, SAML/OIDC federation, MFA, device tracking, advanced security |

## Quickstart

```bash
# start the local server (defaults to 127.0.0.1:4566, in-memory)
openaws serve
# persist to disk instead:
openaws --data-dir ./.openaws serve
```

Talk to it over HTTP:

```bash
# S3: make a bucket, put an object, get it back
curl -X PUT http://127.0.0.1:4566/s3/web
curl -X PUT --data-binary '<h1>hi</h1>' -H 'Content-Type: text/html' \
     http://127.0.0.1:4566/s3/web/index.html
curl http://127.0.0.1:4566/s3/web/index.html

# DynamoDB: create a table with a GSI and put/get an item
curl -X POST http://127.0.0.1:4566/dynamodb \
  -d '{"action":"create_table","name":"orders","hash_key":"id","global_secondary_indexes":[{"name":"by-customer","hash_key":"customer_id"}]}'
curl -X POST http://127.0.0.1:4566/dynamodb \
  -d '{"action":"put_item","table":"orders","item":{"id":"o1","customer_id":"c1"}}'
curl -X POST http://127.0.0.1:4566/dynamodb \
  -d '{"action":"query","table":"orders","key_value":"c1","index_name":"by-customer"}'

# SQS: FIFO queue with message attributes
curl -X POST http://127.0.0.1:4566/sqs \
  -d '{"action":"create_queue","name":"tasks.fifo","fifo":true}'
curl -X POST http://127.0.0.1:4566/sqs \
  -d '{"action":"send_message","queue":"tasks.fifo","body":"job-1","message_group_id":"grp"}'
curl -X POST http://127.0.0.1:4566/sqs \
  -d '{"action":"receive_messages","queue":"tasks.fifo"}'

# SNS: topic + SQS fan-out
curl -X POST http://127.0.0.1:4566/sns -d '{"action":"create_topic","name":"alerts"}'
curl -X POST http://127.0.0.1:4566/sqs -d '{"action":"create_queue","name":"inbox"}'
curl -X POST http://127.0.0.1:4566/sns \
  -d '{"action":"subscribe","topic":"alerts","protocol":"sqs","endpoint":"inbox"}'
curl -X POST http://127.0.0.1:4566/sns \
  -d '{"action":"publish","topic":"alerts","message":"fire!"}'

# EventBridge: route events to SQS
curl -X POST http://127.0.0.1:4566/eventbridge \
  -d '{"action":"put_rule","name":"r1","event_pattern":{"source":["myapp"]}}'
curl -X POST http://127.0.0.1:4566/eventbridge \
  -d '{"action":"put_targets","rule":"r1","targets":[{"id":"t1","type":"sqs","arn":"arn:openaws:sqs:local:000:inbox"}]}'
curl -X POST http://127.0.0.1:4566/eventbridge \
  -d '{"action":"put_events","events":[{"source":"myapp","detail_type":"Alert","detail":{}}]}'

# Step Functions: run a simple Pass machine
curl -X POST http://127.0.0.1:4566/stepfunctions \
  -d '{"action":"create_state_machine","name":"hi","definition":{"StartAt":"S","States":{"S":{"Type":"Pass","Result":{"msg":"hello"},"End":true}}}}'
curl -X POST http://127.0.0.1:4566/stepfunctions \
  -d '{"action":"start_execution","state_machine":"hi"}'

# API Gateway: create a REST API with a mock route and invoke it
curl -X POST http://127.0.0.1:4566/apigateway \
  -d '{"action":"create_rest_api","name":"my-api"}'
# (use the returned id in place of <api_id>)
curl -X POST http://127.0.0.1:4566/apigateway \
  -d '{"action":"create_resource","api_id":"<api_id>","path":"/ping","http_method":"GET","integration_type":"mock"}'
curl http://127.0.0.1:4566/apigw/<api_id>/ping

# SES: capture an email
curl -X POST http://127.0.0.1:4566/ses \
  -d '{"action":"send_email","source":"s@example.com","to_addresses":["r@example.com"],"subject":"hi","body_text":"hello"}'
curl -X POST http://127.0.0.1:4566/ses -d '{"action":"list_emails"}'

# Kinesis: create a stream and put/get records
curl -X POST http://127.0.0.1:4566/kinesis -d '{"action":"create_stream","name":"events","shard_count":2}'
curl -X POST http://127.0.0.1:4566/kinesis \
  -d '{"action":"put_record","stream":"events","data":"aGVsbG8=","partition_key":"pk1"}'
curl -X POST http://127.0.0.1:4566/kinesis \
  -d '{"action":"get_shard_iterator","stream":"events","shard_id":"shardId-000000000000","iterator_type":"TRIM_HORIZON"}'
```

Or use the convenience CLI subcommands:

```bash
openaws --data-dir ./.openaws s3 mb my-bucket
openaws --data-dir ./.openaws s3 put my-bucket key.txt ./local.txt
openaws --data-dir ./.openaws s3 ls my-bucket
openaws --data-dir ./.openaws sqs create jobs
openaws --data-dir ./.openaws sqs send jobs "do the thing"
openaws --data-dir ./.openaws sqs receive jobs
```

Use it as a Python library (no server needed):

```python
from openaws.server import App
import base64

app = App()                      # in-memory; App("./.openaws") to persist

# SQS FIFO with message attributes
app.sqs.create_queue("tasks.fifo", fifo=True)
app.sqs.send_message("tasks.fifo", "job-1", message_group_id="grp",
                     attributes={"priority": {"data_type": "String", "string_value": "high"}})

# SNS fan-out to SQS
app.sns.create_topic("events")
app.sqs.create_queue("inbox")
app.sns.subscribe("events", "sqs", "inbox")
app.sns.publish("events", "something happened")
msg = app.sqs.receive_messages("inbox")[0]
import json; print(json.loads(msg["body"])["Message"])  # 'something happened'

# EventBridge pattern routing
app.eventbridge.put_rule("on-order", event_pattern={"source": ["store"], "detail": {"status": ["placed"]}})
app.eventbridge.put_targets("on-order", targets=[
    {"id": "t1", "type": "sqs", "arn": "arn:openaws:sqs:local:000:inbox"}
])
app.eventbridge.put_events([{"source": "store", "detail_type": "OrderEvent",
                              "detail": {"status": "placed"}}])
print(app.sqs.message_count("inbox"))  # 1

# Step Functions: Choice → Task chain
app.lambdas.register_callable("greet", lambda e, c: f"Hello, {e['name']}!")
app.stepfunctions.create_state_machine("greeter", {
    "StartAt": "Check",
    "States": {
        "Check": {
            "Type": "Choice",
            "Choices": [{"Variable": "$.name", "StringEquals": "World", "Next": "Greet"}],
            "Default": "Greet",
        },
        "Greet": {"Type": "Task", "Resource": "greet", "End": True},
    },
})
result = app.stepfunctions.start_execution("greeter", {"name": "World"})
print(result["output"])  # 'Hello, World!'

# API Gateway
app.lambdas.register_callable("pong", lambda e, c: {"statusCode": 200, "body": "pong", "headers": {}})
api = app.apigateway.create_rest_api("my-api")
app.apigateway.create_resource(api["id"], "/ping", "GET",
                                integration_type="lambda", integration_uri="pong")
resp = app.apigateway.invoke(api["id"], "GET", "/ping")
print(resp["statusCode"], resp["body"])  # 200 pong

# Lambda with env vars, versions, aliases, async invoke
app.lambdas.register_source("processor", """
def handler(event, context):
    return {"stage": context.env.get("STAGE"), "x": event.get("x")}
""", env_vars={"STAGE": "prod"})
print(app.lambdas.invoke("processor", {"x": 42}))  # {'stage': 'prod', 'x': 42}
app.lambdas.publish_version("processor", description="v1")
app.lambdas.create_alias("processor", "live", "1")
app.lambdas.invoke_async("processor", {"x": 1})
print(app.lambdas.process_async_queue("processor"))

# SES email capture
app.ses.send_email("sender@example.com", ["recv@example.com"],
                   "Test subject", body_text="Hello!")
emails = app.ses.list_emails(to_address="recv@example.com")
print(emails[0]["subject"])  # 'Test subject'
```

<!-- cognis:domains:start -->
## Domains

**Primary domain:** Cloud & DevTools  ·  **JTF MERIDIAN division:** ATHENA-PRIME · COGNI-2

**Topics:** `cognis` `devtools` `cloud` `developer-tools` `cloud-emulator`

Part of the **Cognis Neural Suite** — 300+ source-available tools organized across 12 domains under the JTF MERIDIAN command structure. See the [suite on GitHub](https://github.com/cognis-digital) and [jtf-meridian](https://github.com/cognis-digital/jtf-meridian) for how the pieces fit together.
<!-- cognis:domains:end -->

## Install

openaws is **source-available** (it is not published to PyPI). Install it directly
from this Git repository.

### Quick install scripts

```bash
# Linux / macOS
curl -fsSL https://raw.githubusercontent.com/cognis-digital/openaws/main/install.sh | bash
```

```powershell
# Windows (PowerShell)
irm https://raw.githubusercontent.com/cognis-digital/openaws/main/install.ps1 | iex
```

### pipx (recommended — isolated, gives you the `openaws` command)

```bash
pipx install "git+https://github.com/cognis-digital/openaws.git"
```

### uv

```bash
uv tool install "git+https://github.com/cognis-digital/openaws.git"
```

### pip

```bash
pip install "git+https://github.com/cognis-digital/openaws.git"
```

### From source

```bash
git clone https://github.com/cognis-digital/openaws.git
cd openaws
pip install -e ".[dev]"
pytest -q
```

Requires **Python 3.10+**. No third-party runtime dependencies. Verified on Linux,
macOS, and Windows.

## Verification

This project ships a real, end-to-end pytest suite under `tests/`. The suite starts a
real HTTP server in-process and round-trips data through every service (including
cross-service SNS→SQS fan-out, EventBridge→Lambda routing, Step Functions→Lambda
task execution, and API Gateway proxy invocation over the loopback socket) and
exercises the file-backed persistence path across separate `App` instances.

- **382 tests, all passing** (`pytest -q` → `382 passed`), run on Python 3.14 on Windows.
- CI (`.github/workflows/ci.yml`) runs the same suite on **Ubuntu, macOS, and Windows**
  across **Python 3.10–3.13**.

Run it yourself:

```bash
pip install -e ".[dev]"
pytest -q
```

## Topics / Domains

`local-development` · `cloud-emulator` · `aws-compatible` · `s3` · `dynamodb` · `sqs` ·
`lambda` · `kinesis` · `sns` · `eventbridge` · `step-functions` · `api-gateway` · `ses` ·
`iam` · `sts` · `kms` · `secrets-manager` · `ssm` · `cloudwatch` · `cognito` ·
`object-storage` · `key-value-store` · `message-queue` · `serverless` · `data-streams` ·
`identity-and-access` · `key-management` · `observability` ·
`testing` · `offline-development` · `developer-tools`

## Interoperability

`{}` composes with the 300+ tool Cognis suite — JSON in/out and a shared
OpenAI-compatible `/v1` backbone. See **[INTEROP.md](INTEROP.md)** for the
suite map, composition patterns, and reference stacks.

## License

Released under the **Cognis Open Collaboration License (COCL) 1.0** — see
[`LICENSE`](LICENSE).
