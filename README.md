# openaws

## Usage — step by step

`openaws` runs local AWS-style services (S3 / DynamoDB / SQS / Lambda) for
development, plus convenience subcommands for S3 and SQS.

1. **Install** (editable from a clone, or from the wheel):
   ```bash
   pip install -e .
   # provides the `openaws` console script
   ```
2. **Start the local server** (defaults to `127.0.0.1:4566`; pass `--data-dir`
   on the top-level command to persist instead of in-memory):
   ```bash
   openaws --data-dir ./aws-data serve --host 127.0.0.1 --port 4566
   # services: /s3  /dynamodb  /sqs  /lambda
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
queue, a Lambda-style function runner, and a Kinesis Data Streams emulator, all backed
by a local SQLite file (or in-memory for tests).

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
> (S3, DynamoDB, SQS, Lambda, Kinesis, AWS) are used **only nominatively** to describe
> API compatibility. openaws implements a **compatible SUBSET** of each service's
> behaviour and is **NOT for production use**.

## Architecture

openaws is pure Python standard library — no third-party runtime dependencies.

```
openaws/
  storage.py     shared SQLite backend (in-memory or file-backed), thread-safe
  s3.py          S3Service        — buckets, objects, multipart, versioning, tagging, copy, presigned-URL tokens
  dynamodb.py    DynamoDBService  — tables, items, GSI/LSI, conditional writes, batch ops, transactions, TTL, UpdateExpression
  sqs.py         SQSService       — queues & messages (visibility timeout)
  lambdas.py     LambdaService    — register & invoke Python handlers + event sources
  kinesis.py     KinesisService   — streams, shards, put/get records with sequence numbers
  errors.py      shared HTTP-mapped error types
  server.py      one ThreadingHTTPServer exposing every service under path prefixes
  cli.py         `openaws` console entry point (serve + convenience subcommands)
  __main__.py    `python -m openaws`
```

All five services share a single `Storage` instance, so a Lambda function can read an
SQS message, write to a DynamoDB table, and forward an event to Kinesis within one
process — exactly the kind of cross-service flow you want to test locally.

The HTTP server routes by path prefix. S3 is RESTful (binary bodies); the other services
use a tiny JSON "action" protocol (`POST {"action": "...", ...}`).

| Path | Service | Protocol |
| --- | --- | --- |
| `/s3/...` | S3 object store | REST (`PUT`/`GET`/`DELETE`/`GET` list) |
| `/dynamodb` | DynamoDB table store | JSON action |
| `/sqs` | SQS queue | JSON action |
| `/lambda` | Lambda runner | JSON action |
| `/kinesis` | Kinesis Data Streams | JSON action |
| `/` `/health` | health & service listing | `GET` |

## Services

| Service | Class | Implemented | Roadmap (not implemented) |
| --- | --- | --- | --- |
| **S3** | `S3Service` | create/list/delete bucket; put/get/list(+prefix+delimiter)/delete object; MD5 ETags; multipart upload (create/upload-part/complete/abort/list-parts); object versioning (enable/suspend, per-version get/delete, list-versions); object tagging (put/get/delete); object copy (cross-bucket); per-object metadata; presigned-URL HMAC token (generate + verify); prefix+delimiter common-prefixes listing | ACLs, bucket policies, website hosting, lifecycle rules, replication |
| **DynamoDB** | `DynamoDBService` | create/describe/list/delete table; put/get/delete/update item; query (PK + sort `begins_with`/`eq`); scan (+ equality filters); Global Secondary Indexes (GSI); Local Secondary Indexes (LSI); query by index; conditional writes (`attribute_exists`/`attribute_not_exists`/`attribute_equals`); `BatchGetItem`; `BatchWriteItem`; `TransactWriteItems` (all-or-nothing with per-op conditions); TTL (per-table attribute, expired items filtered); `UpdateExpression` subset (SET/REMOVE/ADD) | full expression language, streams, on-demand billing simulation, parallel scan |
| **SQS** | `SQSService` | create/list/delete queue; send/receive/delete message; per-queue visibility timeout + redelivery; receive-count | FIFO guarantees, dead-letter queues, long polling, message attributes |
| **Lambda** | `LambdaService` | register callable or source+handler; synchronous invoke; SQS event-source (poll→invoke→delete); S3 ObjectCreated event | concurrency limits, layers, timeouts, async/destinations |
| **Kinesis Data Streams** | `KinesisService` | create/delete/describe/list stream; configurable shard count; `PutRecord` (bytes or base64, CRC32 shard assignment); `PutRecords` (batch); `GetShardIterator` (TRIM_HORIZON / AT_SEQUENCE_NUMBER / AFTER_SEQUENCE_NUMBER / LATEST); `GetRecords` (paged, with `NextShardIterator`); monotonic sequence numbers; `DescribeStream` shard metadata | enhanced fan-out, server-side encryption, record retention policies, resharding |

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

# SQS: send and receive
curl -X POST http://127.0.0.1:4566/sqs -d '{"action":"create_queue","name":"jobs"}'
curl -X POST http://127.0.0.1:4566/sqs -d '{"action":"send_message","queue":"jobs","body":"hello"}'
curl -X POST http://127.0.0.1:4566/sqs -d '{"action":"receive_messages","queue":"jobs"}'

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

# S3 with versioning
app.s3.create_bucket("data")
app.s3.put_bucket_versioning("data", "enabled")
r1 = app.s3.put_object("data", "report.txt", b"draft 1")
r2 = app.s3.put_object("data", "report.txt", b"final")
print(app.s3.get_object("data", "report.txt", r1["version_id"])["body"])  # b"draft 1"

# DynamoDB with GSI + TTL + TransactWrite
import time
app.dynamodb.create_table("orders", "id", global_secondary_indexes=[
    {"name": "by-customer", "hash_key": "customer_id"}
], ttl_attribute="expires_at")
app.dynamodb.transact_write_items([
    {"put": {"table": "orders", "item": {"id": "o1", "customer_id": "c1",
                                          "expires_at": time.time() + 3600}}},
    {"update": {"table": "orders", "key": {"id": "o1"},
                 "update_expression": {"SET": {"status": "confirmed"}}}},
])

# Kinesis: put and get records
app.kinesis.create_stream("events", shard_count=2)
app.kinesis.put_record("events", b"user-clicked", "user-123")
it = app.kinesis.get_shard_iterator("events", "shardId-000000000000", "TRIM_HORIZON")
result = app.kinesis.get_records(it["shard_iterator"])
print(base64.b64decode(result["records"][0]["data"]))  # b"user-clicked"

# Lambda + SQS (unchanged)
app.sqs.create_queue("work")
app.sqs.send_message("work", "task-1")
app.lambdas.register_callable(
    "worker", lambda event, ctx: [r["body"].upper() for r in event["Records"]]
)
print(app.lambdas.invoke_from_sqs("worker", app.sqs, "work"))  # [['TASK-1']]
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
real HTTP server in-process and round-trips data through every service (including a
cross-service SQS→Lambda flow over the loopback socket) and exercises the file-backed
persistence path across separate `App` instances.

- **150 tests, all passing** (`pytest -q` → `150 passed`), run on Python 3.14 on Windows.
- CI (`.github/workflows/ci.yml`) runs the same suite on **Ubuntu, macOS, and Windows**
  across **Python 3.10–3.13**.

Run it yourself:

```bash
pip install -e ".[dev]"
pytest -q
```

## Topics / Domains

`local-development` · `cloud-emulator` · `aws-compatible` · `s3` · `dynamodb` · `sqs` ·
`lambda` · `kinesis` · `object-storage` · `key-value-store` · `message-queue` ·
`serverless` · `data-streams` · `testing` · `offline-development` · `developer-tools`

## License

Released under the **Cognis Open Collaboration License (COCL) 1.0** — see
[`LICENSE`](LICENSE).
