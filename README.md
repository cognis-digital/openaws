# openaws

## What is this?

**openaws** is an independent, open-source tool that runs a small set of **AWS-style
cloud services on your own machine** — entirely offline. You start one local server
and get an S3-style object store, a DynamoDB-style table store, an SQS-style message
queue, and a Lambda-style function runner, all backed by a local SQLite file (or
in-memory for tests).

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
> (S3, DynamoDB, SQS, Lambda, AWS) are used **only nominatively** to describe API
> compatibility. openaws implements a **compatible SUBSET** of each service's behaviour
> and is **NOT for production use**.

## Architecture

openaws is pure Python standard library — no third-party runtime dependencies.

```
openaws/
  storage.py     shared SQLite backend (in-memory or file-backed), thread-safe
  s3.py          S3Service        — buckets & objects
  dynamodb.py    DynamoDBService  — tables & items (PK + optional SK)
  sqs.py         SQSService       — queues & messages (visibility timeout)
  lambdas.py     LambdaService    — register & invoke Python handlers + event sources
  errors.py      shared HTTP-mapped error types
  server.py      one ThreadingHTTPServer exposing every service under path prefixes
  cli.py         `openaws` console entry point (serve + convenience subcommands)
  __main__.py    `python -m openaws`
```

All four services share a single `Storage` instance, so a Lambda function can read an
SQS message and write to a DynamoDB table within one process — exactly the kind of
cross-service flow you want to test locally.

The HTTP server routes by path prefix. S3 is RESTful (binary bodies); the other three
use a tiny JSON "action" protocol (`POST {"action": "...", ...}`).

| Path | Service | Protocol |
| --- | --- | --- |
| `/s3/...` | S3 object store | REST (`PUT`/`GET`/`DELETE`/`GET` list) |
| `/dynamodb` | DynamoDB table store | JSON action |
| `/sqs` | SQS queue | JSON action |
| `/lambda` | Lambda runner | JSON action |
| `/` `/health` | health & service listing | `GET` |

## Services

| Service | Class | Implemented | Roadmap (not implemented) |
| --- | --- | --- | --- |
| **S3** | `S3Service` | create/list/delete bucket; put/get/list(+prefix)/delete object; MD5 ETags | multipart upload, versioning, ACLs, presigned URLs |
| **DynamoDB** | `DynamoDBService` | create/describe/list/delete table; put/get/delete item; query (PK + sort `begins_with`/`eq`); scan (+ equality filters); partition key + optional sort key | secondary indexes, conditional writes, full expression language |
| **SQS** | `SQSService` | create/list/delete queue; send/receive/delete message; per-queue visibility timeout + redelivery; receive-count | FIFO guarantees, dead-letter queues, long polling, message attributes |
| **Lambda** | `LambdaService` | register callable or source+handler; synchronous invoke; SQS event-source (poll→invoke→delete); S3 ObjectCreated event | concurrency limits, layers, timeouts, async/destinations |

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

# DynamoDB: create a table and put/get an item
curl -X POST http://127.0.0.1:4566/dynamodb \
  -d '{"action":"create_table","name":"users","hash_key":"id"}'
curl -X POST http://127.0.0.1:4566/dynamodb \
  -d '{"action":"put_item","table":"users","item":{"id":"u1","name":"Ada"}}'
curl -X POST http://127.0.0.1:4566/dynamodb \
  -d '{"action":"get_item","table":"users","key":{"id":"u1"}}'

# SQS: send and receive
curl -X POST http://127.0.0.1:4566/sqs -d '{"action":"create_queue","name":"jobs"}'
curl -X POST http://127.0.0.1:4566/sqs -d '{"action":"send_message","queue":"jobs","body":"hello"}'
curl -X POST http://127.0.0.1:4566/sqs -d '{"action":"receive_messages","queue":"jobs"}'
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

app = App()                      # in-memory; App("./.openaws") to persist
app.s3.create_bucket("data")
app.s3.put_object("data", "hello.txt", b"hi")
print(app.s3.get_object("data", "hello.txt")["body"])  # b"hi"

app.sqs.create_queue("work")
app.sqs.send_message("work", "task-1")
app.lambdas.register_callable(
    "worker", lambda event, ctx: [r["body"].upper() for r in event["Records"]]
)
print(app.lambdas.invoke_from_sqs("worker", app.sqs, "work"))  # [['TASK-1']]
```

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

- **53 tests, all passing** (`pytest -q` → `53 passed`), run on Python 3.14 on Windows.
- CI (`.github/workflows/ci.yml`) runs the same suite on **Ubuntu, macOS, and Windows**
  across **Python 3.10–3.13**.

Run it yourself:

```bash
pip install -e ".[dev]"
pytest -q
```

## Topics / Domains

`local-development` · `cloud-emulator` · `aws-compatible` · `s3` · `dynamodb` · `sqs` ·
`lambda` · `object-storage` · `key-value-store` · `message-queue` · `serverless` ·
`testing` · `offline-development` · `developer-tools`

## License

Released under the **Cognis Open Collaboration License (COCL) 1.0** — see
[`LICENSE`](LICENSE).
