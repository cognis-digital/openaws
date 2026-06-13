"""openaws — an independent, open-source LOCAL reimplementation of core AWS primitives.

openaws gives developers a single local HTTP server (and Python API) that emulates a
useful SUBSET of ten AWS-style services for offline development and testing:

    * S3              — object store (buckets, objects, multipart, versioning, tagging, copy,
                        presigned-URL tokens)
    * DynamoDB        — key/value table store (PK + optional SK, GSI/LSI, conditional writes,
                        BatchGet/Write, TransactWrite, TTL, UpdateExpression subset)
    * SQS             — message queue (visibility timeout, FIFO, DLQ redrive, message attributes)
    * Lambda          — Python function runner (sync/async invoke, env vars, versions, aliases,
                        layers metadata, event sources)
    * Kinesis Streams — stream/shard/put/get records with sequence numbers
    * SNS             — pub/sub topics, subscriptions, fan-out to SQS/Lambda
    * EventBridge     — event buses, rules with pattern matching, targets to Lambda/SQS
    * Step Functions  — state machine definition + synchronous execution (Task/Choice/Pass/Wait/Parallel)
    * API Gateway     — REST APIs, routes, Lambda/mock integration, invocation
    * SES             — email capture (send-email stored locally, list/get by recipient)

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
from .sns import SNSService
from .eventbridge import EventBridgeService
from .stepfunctions import StepFunctionsService
from .apigateway import APIGatewayService
from .ses import SESService

__version__ = "0.3.0"

__all__ = [
    "Storage",
    "S3Service",
    "DynamoDBService",
    "SQSService",
    "LambdaService",
    "KinesisService",
    "SNSService",
    "EventBridgeService",
    "StepFunctionsService",
    "APIGatewayService",
    "SESService",
    "__version__",
]
