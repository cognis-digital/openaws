"""Command-line interface for openaws.

Subcommands:
    serve        start the local HTTP server (all services)
    s3           bucket/object convenience operations
    sqs          queue convenience operations
    version      print the version
"""

from __future__ import annotations

import argparse
import sys

from . import __version__
from .server import App, OpenAWSServer


def _cmd_serve(args) -> int:
    server = OpenAWSServer(host=args.host, port=args.port, data_dir=args.data_dir)
    print(f"openaws {__version__} listening on {server.base_url}")
    print(f"data dir: {args.data_dir or ':memory:'}")
    print("services: /s3  /dynamodb  /sqs  /lambda")
    try:
        server.serve_forever()
    except KeyboardInterrupt:  # pragma: no cover - interactive
        print("\nshutting down")
        server.stop()
    return 0


def _cmd_s3(args) -> int:
    app = App(args.data_dir)
    if args.s3_cmd == "mb":
        app.s3.create_bucket(args.bucket)
        print(f"created bucket {args.bucket}")
    elif args.s3_cmd == "ls":
        if args.bucket:
            result = app.s3.list_objects(args.bucket)
            for o in result["objects"]:
                print(f"{o['size']:>10}  {o['key']}")
        else:
            for b in app.s3.list_buckets():
                print(b["name"])
    elif args.s3_cmd == "put":
        with open(args.file, "rb") as fh:
            data = fh.read()
        res = app.s3.put_object(args.bucket, args.key, data)
        print(f"put {args.bucket}/{args.key} etag={res['etag']}")
    elif args.s3_cmd == "get":
        obj = app.s3.get_object(args.bucket, args.key)
        sys.stdout.buffer.write(obj["body"])
    return 0


def _cmd_sqs(args) -> int:
    app = App(args.data_dir)
    if args.sqs_cmd == "create":
        app.sqs.create_queue(args.queue)
        print(f"created queue {args.queue}")
    elif args.sqs_cmd == "send":
        res = app.sqs.send_message(args.queue, args.body)
        print(res["message_id"])
    elif args.sqs_cmd == "receive":
        for m in app.sqs.receive_messages(args.queue, args.max):
            print(f"{m['message_id']}\t{m['body']}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="openaws", description="Local AWS-style services for development.")
    p.add_argument("--data-dir", default=None, help="persist to this dir (default: in-memory)")
    sub = p.add_subparsers(dest="command", required=True)

    serve = sub.add_parser("serve", help="start the local HTTP server")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=4566)
    serve.set_defaults(func=_cmd_serve)

    s3 = sub.add_parser("s3", help="S3 convenience commands")
    s3sub = s3.add_subparsers(dest="s3_cmd", required=True)
    mb = s3sub.add_parser("mb", help="make bucket")
    mb.add_argument("bucket")
    ls = s3sub.add_parser("ls", help="list buckets or objects")
    ls.add_argument("bucket", nargs="?")
    put = s3sub.add_parser("put", help="put object from file")
    put.add_argument("bucket"); put.add_argument("key"); put.add_argument("file")
    get = s3sub.add_parser("get", help="get object to stdout")
    get.add_argument("bucket"); get.add_argument("key")
    s3.set_defaults(func=_cmd_s3)

    sqs = sub.add_parser("sqs", help="SQS convenience commands")
    sqssub = sqs.add_subparsers(dest="sqs_cmd", required=True)
    cq = sqssub.add_parser("create", help="create queue"); cq.add_argument("queue")
    sm = sqssub.add_parser("send", help="send message")
    sm.add_argument("queue"); sm.add_argument("body")
    rm = sqssub.add_parser("receive", help="receive messages")
    rm.add_argument("queue"); rm.add_argument("--max", type=int, default=1)
    sqs.set_defaults(func=_cmd_sqs)

    ver = sub.add_parser("version", help="print version")
    ver.set_defaults(func=lambda a: (print(__version__), 0)[1])

    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
