"""openaws — an independent, open-source LOCAL reimplementation of core AWS primitives.

openaws gives developers a single local HTTP server (and Python API) that emulates a
useful SUBSET of five AWS-style services for offline development and testing:

    * S3              — object store (buckets, objects, multipart, versioning, tagging, copy,
                        presigned-URL tokens)
    * DynamoDB        — key/value table store (PK + optional SK, GSI/LSI, conditional writes,
                        BatchGet/Write, TransactWrite, TTL, UpdateExpression subset)
    * SQS             — message queue (visibility timeout)
    * Lambda          — Python function runner (sync invoke + event sources)
    * Kinesis Streams — stream/shard/put/get records with sequence numbers

DISCLAIMER: openaws is an independent open reimplementation for LOCAL development.
It is NOT affiliated with, endorsed by, or sponsored by Amazon Web Services or any
vendor. Vendor and service names are used only nominatively to describe API
compatibility. openaws implements a compatible SUBSET and is NOT for production use.
"""

from .storage import Storage
from .s3 import S3Service
from .dynamodb import DynamoDBService
from .sqs import SQSService
from .lambdas import LambdaService
from .kinesis import KinesisService

__version__ = "0.2.0"

__all__ = [
    "Storage",
    "S3Service",
    "DynamoDBService",
    "SQSService",
    "LambdaService",
    "KinesisService",
    "__version__",
]
