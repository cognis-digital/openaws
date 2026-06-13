"""Single local HTTP server exposing every openaws service.

Routing is path-prefix based so each service lives under a clear namespace:

    /s3/...         S3 object store
    /dynamodb       DynamoDB-style table store (JSON action API)
    /sqs            SQS-style queue (JSON action API)
    /lambda         Lambda-style runner (JSON action API)
    /kinesis        Kinesis Data Streams (JSON action API)
    /sns            SNS pub/sub (JSON action API)
    /eventbridge    EventBridge event buses / rules / targets (JSON action API)
    /stepfunctions  Step Functions state machines + executions (JSON action API)
    /apigateway     API Gateway management (JSON action API)
    /apigw/...      API Gateway invocation (REST: /<api_id>/<path>)
    /ses            SES email capture (JSON action API)
    /               health / service listing

S3 uses RESTful paths (``/s3/<bucket>/<key>``) because objects are binary;
the other services use a small JSON action protocol (POST a body containing an
``"action"`` plus its parameters) which keeps the client surface tiny while
remaining easy to script against.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlparse

from . import __version__
from .apigateway import APIGatewayService
from .cloudwatch import CloudWatchService
from .cognito import CognitoService
from .dynamodb import DynamoDBService
from .errors import OpenAWSError, ValidationError
from .eventbridge import EventBridgeService
from .iam import IAMService
from .kinesis import KinesisService
from .kms import KMSService
from .lambdas import LambdaService
from .s3 import S3Service
from .secretsmanager import SecretsManagerService
from .ses import SESService
from .sns import SNSService
from .sqs import SQSService
from .ssm import SSMService
from .stepfunctions import StepFunctionsService
from .sts import STSService
from .storage import Storage


class App:
    """Holds the service instances backed by one Storage."""

    def __init__(self, data_dir: str | None = None, storage: Storage | None = None):
        self.storage = storage or Storage(data_dir)
        self.s3 = S3Service(self.storage)
        self.dynamodb = DynamoDBService(self.storage)
        self.sqs = SQSService(self.storage)
        self.lambdas = LambdaService(self.storage)
        self.kinesis = KinesisService(self.storage)
        self.sns = SNSService(self.storage)
        self.eventbridge = EventBridgeService(self.storage)
        self.stepfunctions = StepFunctionsService(self.storage)
        self.apigateway = APIGatewayService(self.storage)
        self.ses = SESService(self.storage)
        self.iam = IAMService(self.storage)
        self.sts = STSService(self.storage)
        self.kms = KMSService(self.storage)
        self.secretsmanager = SecretsManagerService(self.storage)
        self.ssm = SSMService(self.storage, kms_service=self.kms)
        self.cloudwatch = CloudWatchService(self.storage)
        self.cognito = CognitoService(self.storage)

        # wire cross-service references for fan-out
        self.sns._sqs = self.sqs
        self.sns._lambdas = self.lambdas
        self.eventbridge._sqs = self.sqs
        self.eventbridge._lambdas = self.lambdas
        self.stepfunctions._lambdas = self.lambdas
        self.apigateway._lambdas = self.lambdas


def _dispatch_dynamodb(app: App, payload: dict):
    action = payload.get("action")
    d = app.dynamodb
    if action == "create_table":
        return d.create_table(
            payload["name"],
            payload["hash_key"],
            payload.get("range_key"),
            payload.get("global_secondary_indexes"),
            payload.get("local_secondary_indexes"),
            payload.get("ttl_attribute"),
        )
    if action == "list_tables":
        return {"tables": d.list_tables()}
    if action == "describe_table":
        return d.describe_table(payload["name"])
    if action == "delete_table":
        d.delete_table(payload["name"])
        return {"deleted": payload["name"]}
    if action == "update_ttl":
        return d.update_ttl(payload["table"], payload.get("ttl_attribute"))
    if action == "put_item":
        return {"item": d.put_item(
            payload["table"], payload["item"], payload.get("condition")
        )}
    if action == "get_item":
        return {"item": d.get_item(payload["table"], payload["key"])}
    if action == "delete_item":
        d.delete_item(payload["table"], payload["key"], payload.get("condition"))
        return {"deleted": True}
    if action == "update_item":
        return {"item": d.update_item(
            payload["table"],
            payload["key"],
            payload["update_expression"],
            payload.get("condition"),
        )}
    if action == "query":
        return {
            "items": d.query(
                payload["table"],
                payload["key_value"],
                payload.get("sort_begins_with"),
                payload.get("sort_eq"),
                payload.get("index_name"),
            )
        }
    if action == "scan":
        return {"items": d.scan(payload["table"], payload.get("filters"))}
    if action == "batch_get_item":
        return {"responses": d.batch_get_item(payload["request_items"])}
    if action == "batch_write_item":
        return d.batch_write_item(payload["request_items"])
    if action == "transact_write_items":
        return d.transact_write_items(payload["transact_items"])
    raise ValidationError(f"unknown dynamodb action: {action!r}")


def _dispatch_sqs(app: App, payload: dict):
    action = payload.get("action")
    q = app.sqs
    if action == "create_queue":
        return q.create_queue(
            payload["name"],
            payload.get("visibility_timeout", 30.0),
            fifo=payload.get("fifo", False),
            dedup_window=payload.get("dedup_window", 300.0),
            dlq_name=payload.get("dlq_name"),
            max_receive_count=payload.get("max_receive_count", 0),
        )
    if action == "list_queues":
        return {"queues": q.list_queues()}
    if action == "delete_queue":
        q.delete_queue(payload["name"])
        return {"deleted": payload["name"]}
    if action == "send_message":
        return q.send_message(
            payload["queue"],
            payload["body"],
            message_group_id=payload.get("message_group_id"),
            message_deduplication_id=payload.get("message_deduplication_id"),
            attributes=payload.get("attributes"),
        )
    if action == "receive_messages":
        return {
            "messages": q.receive_messages(
                payload["queue"], payload.get("max_messages", 1)
            )
        }
    if action == "delete_message":
        return {"deleted": q.delete_message(payload["queue"], payload["receipt_handle"])}
    if action == "message_count":
        return {"count": q.message_count(payload["queue"])}
    if action == "get_queue_attributes":
        return q.get_queue_attributes(payload["name"])
    raise ValidationError(f"unknown sqs action: {action!r}")


def _dispatch_lambda(app: App, payload: dict):
    action = payload.get("action")
    fn = app.lambdas
    if action == "register_source":
        return fn.register_source(
            payload["name"],
            payload["source"],
            payload.get("handler", "handler"),
            env_vars=payload.get("env_vars"),
            description=payload.get("description", ""),
            timeout=payload.get("timeout", 3),
        )
    if action == "list_functions":
        return {"functions": fn.list_functions()}
    if action == "delete_function":
        fn.delete_function(payload["name"])
        return {"deleted": payload["name"]}
    if action == "invoke":
        return {"result": fn.invoke(payload["name"], payload.get("event"))}
    if action == "invoke_async":
        return fn.invoke_async(payload["name"], payload.get("event"))
    if action == "process_async_queue":
        return {"results": fn.process_async_queue(payload.get("function_name"))}
    if action == "list_async_invocations":
        return {"invocations": fn.list_async_invocations(payload.get("function_name"))}
    if action == "get_function":
        return fn.get_function(payload["name"])
    if action == "update_function_configuration":
        return fn.update_function_configuration(
            payload["name"],
            env_vars=payload.get("env_vars"),
            description=payload.get("description"),
            timeout=payload.get("timeout"),
        )
    if action == "publish_version":
        return fn.publish_version(payload["name"], payload.get("description", ""))
    if action == "list_versions":
        return {"versions": fn.list_versions(payload["name"])}
    if action == "create_alias":
        return fn.create_alias(
            payload["name"],
            payload["alias"],
            payload["version"],
            payload.get("description", ""),
        )
    if action == "update_alias":
        return fn.update_alias(
            payload["name"],
            payload["alias"],
            payload["version"],
            payload.get("description"),
        )
    if action == "list_aliases":
        return {"aliases": fn.list_aliases(payload["name"])}
    if action == "delete_alias":
        fn.delete_alias(payload["name"], payload["alias"])
        return {"deleted": payload["alias"]}
    if action == "add_layer_version":
        return fn.add_layer_version(
            payload["name"],
            payload.get("description", ""),
            payload.get("compatible_runtimes"),
        )
    if action == "list_layer_versions":
        return {"versions": fn.list_layer_versions(payload["name"])}
    if action == "list_layers":
        return {"layers": fn.list_layers()}
    if action == "invoke_from_sqs":
        return {
            "results": fn.invoke_from_sqs(
                payload["name"], app.sqs, payload["queue"], payload.get("max_messages", 10)
            )
        }
    if action == "invoke_from_s3_put":
        return {
            "result": fn.invoke_from_s3_put(
                payload["name"], payload["bucket"], payload["key"], payload.get("size", 0)
            )
        }
    raise ValidationError(f"unknown lambda action: {action!r}")


def _dispatch_kinesis(app: App, payload: dict):
    action = payload.get("action")
    k = app.kinesis
    if action == "create_stream":
        return k.create_stream(payload["name"], payload.get("shard_count", 1))
    if action == "delete_stream":
        k.delete_stream(payload["name"])
        return {"deleted": payload["name"]}
    if action == "describe_stream":
        return k.describe_stream(payload["name"])
    if action == "list_streams":
        return {"streams": k.list_streams()}
    if action == "put_record":
        return k.put_record(
            payload["stream"],
            payload["data"],
            payload["partition_key"],
            payload.get("explicit_hash_key"),
        )
    if action == "put_records":
        return k.put_records(payload["stream"], payload["records"])
    if action == "get_shard_iterator":
        return k.get_shard_iterator(
            payload["stream"],
            payload["shard_id"],
            payload["iterator_type"],
            payload.get("starting_sequence_number"),
        )
    if action == "get_records":
        return k.get_records(payload["shard_iterator"], payload.get("limit", 100))
    raise ValidationError(f"unknown kinesis action: {action!r}")


def _dispatch_sns(app: App, payload: dict):
    action = payload.get("action")
    s = app.sns
    if action == "create_topic":
        return s.create_topic(payload["name"])
    if action == "list_topics":
        return {"topics": s.list_topics()}
    if action == "delete_topic":
        s.delete_topic(payload["name"])
        return {"deleted": payload["name"]}
    if action == "subscribe":
        return s.subscribe(payload["topic"], payload["protocol"], payload["endpoint"])
    if action == "list_subscriptions":
        return {"subscriptions": s.list_subscriptions(payload.get("topic"))}
    if action == "unsubscribe":
        s.unsubscribe(payload["subscription_arn"])
        return {"unsubscribed": payload["subscription_arn"]}
    if action == "publish":
        return s.publish(
            payload["topic"],
            payload["message"],
            payload.get("subject"),
            payload.get("attributes"),
        )
    if action == "get_deliveries":
        return {"deliveries": s.get_deliveries(payload["topic"])}
    raise ValidationError(f"unknown sns action: {action!r}")


def _dispatch_eventbridge(app: App, payload: dict):
    action = payload.get("action")
    eb = app.eventbridge
    if action == "create_event_bus":
        return eb.create_event_bus(payload["name"])
    if action == "list_event_buses":
        return {"buses": eb.list_event_buses()}
    if action == "delete_event_bus":
        eb.delete_event_bus(payload["name"])
        return {"deleted": payload["name"]}
    if action == "put_rule":
        return eb.put_rule(
            payload["name"],
            bus=payload.get("bus", "default"),
            event_pattern=payload.get("event_pattern"),
            schedule_expression=payload.get("schedule_expression"),
            state=payload.get("state", "ENABLED"),
        )
    if action == "list_rules":
        return {"rules": eb.list_rules(payload.get("bus", "default"))}
    if action == "delete_rule":
        eb.delete_rule(payload["name"], payload.get("bus", "default"))
        return {"deleted": payload["name"]}
    if action == "put_targets":
        return eb.put_targets(
            payload["rule"],
            bus=payload.get("bus", "default"),
            targets=payload.get("targets", []),
        )
    if action == "list_targets":
        return {"targets": eb.list_targets(payload["rule"], payload.get("bus", "default"))}
    if action == "remove_targets":
        eb.remove_targets(
            payload["rule"],
            bus=payload.get("bus", "default"),
            ids=payload.get("ids", []),
        )
        return {"removed": payload.get("ids", [])}
    if action == "put_events":
        return eb.put_events(payload.get("events", []))
    raise ValidationError(f"unknown eventbridge action: {action!r}")


def _dispatch_stepfunctions(app: App, payload: dict):
    action = payload.get("action")
    sf = app.stepfunctions
    if action == "create_state_machine":
        return sf.create_state_machine(payload["name"], payload["definition"])
    if action == "list_state_machines":
        return {"state_machines": sf.list_state_machines()}
    if action == "describe_state_machine":
        return sf.describe_state_machine(payload["name"])
    if action == "delete_state_machine":
        sf.delete_state_machine(payload["name"])
        return {"deleted": payload["name"]}
    if action == "start_execution":
        return sf.start_execution(
            payload["state_machine"],
            payload.get("input"),
            payload.get("execution_name"),
        )
    if action == "list_executions":
        return {"executions": sf.list_executions(payload["state_machine"])}
    if action == "describe_execution":
        return sf.describe_execution(payload["execution_arn"])
    raise ValidationError(f"unknown stepfunctions action: {action!r}")


def _dispatch_apigateway(app: App, payload: dict):
    action = payload.get("action")
    ag = app.apigateway
    if action == "create_rest_api":
        return ag.create_rest_api(payload["name"], payload.get("description", ""))
    if action == "list_rest_apis":
        return {"apis": ag.list_rest_apis()}
    if action == "delete_rest_api":
        ag.delete_rest_api(payload["api_id"])
        return {"deleted": payload["api_id"]}
    if action == "create_resource":
        return ag.create_resource(
            payload["api_id"],
            payload["path"],
            payload["http_method"],
            payload.get("integration_type", "lambda"),
            payload.get("integration_uri", ""),
        )
    if action == "list_resources":
        return {"resources": ag.list_resources(payload["api_id"])}
    if action == "delete_resource":
        ag.delete_resource(payload["api_id"], payload["resource_id"])
        return {"deleted": payload["resource_id"]}
    if action == "invoke":
        return ag.invoke(
            payload["api_id"],
            payload["http_method"],
            payload["path"],
            body=payload.get("body"),
            query_params=payload.get("query_params"),
            headers=payload.get("headers"),
        )
    raise ValidationError(f"unknown apigateway action: {action!r}")


def _dispatch_ses(app: App, payload: dict):
    action = payload.get("action")
    se = app.ses
    if action == "verify_email_identity":
        return se.verify_email_identity(payload["email"])
    if action == "list_identities":
        return {"identities": se.list_identities()}
    if action == "delete_identity":
        se.delete_identity(payload["email"])
        return {"deleted": payload["email"]}
    if action == "send_email":
        return se.send_email(
            payload["source"],
            payload["to_addresses"],
            payload["subject"],
            body_text=payload.get("body_text"),
            body_html=payload.get("body_html"),
            cc_addresses=payload.get("cc_addresses"),
            bcc_addresses=payload.get("bcc_addresses"),
            reply_to=payload.get("reply_to"),
        )
    if action == "list_emails":
        return {
            "emails": se.list_emails(
                to_address=payload.get("to_address"),
                limit=payload.get("limit", 50),
            )
        }
    if action == "get_email":
        return se.get_email(payload["msg_id"])
    if action == "delete_emails":
        return {"deleted": se.delete_emails()}
    raise ValidationError(f"unknown ses action: {action!r}")


def _dispatch_iam(app: App, payload: dict):
    action = payload.get("action")
    im = app.iam
    if action == "create_user":
        return im.create_user(payload["username"], payload.get("path", "/"))
    if action == "get_user":
        return im.get_user(payload["username"])
    if action == "delete_user":
        im.delete_user(payload["username"])
        return {"deleted": payload["username"]}
    if action == "list_users":
        return {"users": im.list_users(payload.get("path_prefix", "/"))}
    if action == "create_access_key":
        return im.create_access_key(payload["username"])
    if action == "list_access_keys":
        return {"access_keys": im.list_access_keys(payload["username"])}
    if action == "delete_access_key":
        im.delete_access_key(payload["key_id"])
        return {"deleted": payload["key_id"]}
    if action == "create_group":
        return im.create_group(payload["group_name"], payload.get("path", "/"))
    if action == "delete_group":
        im.delete_group(payload["group_name"])
        return {"deleted": payload["group_name"]}
    if action == "list_groups":
        return {"groups": im.list_groups()}
    if action == "add_user_to_group":
        im.add_user_to_group(payload["username"], payload["group_name"])
        return {"added": True}
    if action == "remove_user_from_group":
        im.remove_user_from_group(payload["username"], payload["group_name"])
        return {"removed": True}
    if action == "list_groups_for_user":
        return {"groups": im.list_groups_for_user(payload["username"])}
    if action == "create_role":
        return im.create_role(
            payload["role_name"],
            payload["assume_role_policy"],
            payload.get("description", ""),
            payload.get("path", "/"),
        )
    if action == "get_role":
        return im.get_role(payload["role_name"])
    if action == "delete_role":
        im.delete_role(payload["role_name"])
        return {"deleted": payload["role_name"]}
    if action == "list_roles":
        return {"roles": im.list_roles()}
    if action == "create_policy":
        return im.create_policy(
            payload["policy_name"],
            payload["policy_document"],
            payload.get("description", ""),
            payload.get("path", "/"),
        )
    if action == "get_policy":
        return im.get_policy(payload["policy_name"])
    if action == "delete_policy":
        im.delete_policy(payload["policy_name"])
        return {"deleted": payload["policy_name"]}
    if action == "list_policies":
        return {"policies": im.list_policies(payload.get("scope", "Local"))}
    if action == "put_inline_policy":
        im.put_inline_policy(
            payload["principal_type"], payload["principal_name"],
            payload["policy_name"], payload["policy_document"],
        )
        return {"put": True}
    if action == "get_inline_policy":
        return im.get_inline_policy(
            payload["principal_type"], payload["principal_name"], payload["policy_name"]
        )
    if action == "delete_inline_policy":
        im.delete_inline_policy(
            payload["principal_type"], payload["principal_name"], payload["policy_name"]
        )
        return {"deleted": True}
    if action == "list_inline_policies":
        return {"policy_names": im.list_inline_policies(
            payload["principal_type"], payload["principal_name"]
        )}
    if action == "attach_policy":
        im.attach_policy(
            payload["principal_type"], payload["principal_name"], payload["policy_name"]
        )
        return {"attached": True}
    if action == "detach_policy":
        im.detach_policy(
            payload["principal_type"], payload["principal_name"], payload["policy_name"]
        )
        return {"detached": True}
    if action == "list_attached_policies":
        return {"policy_names": im.list_attached_policies(
            payload["principal_type"], payload["principal_name"]
        )}
    if action == "simulate_principal_policy":
        return {"evaluation_results": im.simulate_principal_policy(
            payload["principal_type"], payload["principal_name"],
            payload["action_names"], payload.get("resource_arns"),
        )}
    raise ValidationError(f"unknown iam action: {action!r}")


def _dispatch_sts(app: App, payload: dict):
    action = payload.get("action")
    st = app.sts
    if action == "assume_role":
        return st.assume_role(
            payload["role_arn"],
            payload["role_session_name"],
            payload.get("duration_seconds", 3600),
            payload.get("external_id"),
        )
    if action == "get_session_token":
        return st.get_session_token(payload.get("duration_seconds", 43200))
    if action == "get_caller_identity":
        return st.get_caller_identity(payload.get("access_key_id"))
    if action == "list_sessions":
        return {"sessions": st.list_sessions()}
    if action == "revoke_session":
        st.revoke_session(payload["access_key_id"])
        return {"revoked": True}
    raise ValidationError(f"unknown sts action: {action!r}")


def _dispatch_kms(app: App, payload: dict):
    action = payload.get("action")
    km = app.kms
    if action == "create_key":
        return km.create_key(
            payload.get("description", ""),
            payload.get("key_usage", "ENCRYPT_DECRYPT"),
            payload.get("key_spec", "SYMMETRIC_DEFAULT"),
        )
    if action == "describe_key":
        return km.describe_key(payload["key_id"])
    if action == "list_keys":
        return {"keys": km.list_keys()}
    if action == "schedule_key_deletion":
        return km.schedule_key_deletion(
            payload["key_id"], payload.get("pending_window_in_days", 30)
        )
    if action == "disable_key":
        km.disable_key(payload["key_id"])
        return {"disabled": True}
    if action == "enable_key":
        km.enable_key(payload["key_id"])
        return {"enabled": True}
    if action == "encrypt":
        import base64
        plaintext = payload["plaintext"]
        if isinstance(plaintext, str):
            plaintext_bytes = base64.b64decode(plaintext) if payload.get("plaintext_is_b64") else plaintext.encode()
        else:
            plaintext_bytes = str(plaintext).encode()
        return km.encrypt(payload["key_id"], plaintext_bytes, payload.get("encryption_context"))
    if action == "decrypt":
        return km.decrypt(payload["ciphertext_blob"], payload.get("encryption_context"))
    if action == "generate_data_key":
        return km.generate_data_key(
            payload["key_id"],
            payload.get("key_spec", "AES_256"),
            payload.get("number_of_bytes"),
            payload.get("encryption_context"),
        )
    if action == "generate_data_key_without_plaintext":
        return km.generate_data_key_without_plaintext(
            payload["key_id"],
            payload.get("key_spec", "AES_256"),
            payload.get("number_of_bytes"),
            payload.get("encryption_context"),
        )
    if action == "create_alias":
        km.create_alias(payload["alias_name"], payload["target_key_id"])
        return {"created": True}
    if action == "list_aliases":
        return {"aliases": km.list_aliases()}
    if action == "delete_alias":
        km.delete_alias(payload["alias_name"])
        return {"deleted": True}
    if action == "enable_key_rotation":
        km.enable_key_rotation(payload["key_id"])
        return {"enabled": True}
    if action == "disable_key_rotation":
        km.disable_key_rotation(payload["key_id"])
        return {"disabled": True}
    if action == "get_key_rotation_status":
        return km.get_key_rotation_status(payload["key_id"])
    raise ValidationError(f"unknown kms action: {action!r}")


def _dispatch_secretsmanager(app: App, payload: dict):
    action = payload.get("action")
    sm = app.secretsmanager
    if action == "create_secret":
        binary = payload.get("secret_binary")
        if binary and isinstance(binary, str):
            import base64
            binary = base64.b64decode(binary)
        return sm.create_secret(
            payload["name"],
            secret_string=payload.get("secret_string"),
            secret_binary=binary,
            description=payload.get("description", ""),
            tags=payload.get("tags"),
        )
    if action == "describe_secret":
        return sm.describe_secret(payload["name"])
    if action == "list_secrets":
        return {"secrets": sm.list_secrets()}
    if action == "update_secret":
        return sm.update_secret(
            payload["name"],
            description=payload.get("description"),
            secret_string=payload.get("secret_string"),
        )
    if action == "delete_secret":
        return sm.delete_secret(
            payload["name"],
            payload.get("recovery_window_in_days", 30),
            payload.get("force_delete_without_recovery", False),
        )
    if action == "restore_secret":
        return sm.restore_secret(payload["name"])
    if action == "put_secret_value":
        return sm.put_secret_value(
            payload["name"],
            secret_string=payload.get("secret_string"),
            client_request_token=payload.get("client_request_token"),
            version_stages=payload.get("version_stages"),
        )
    if action == "get_secret_value":
        return sm.get_secret_value(
            payload["name"],
            version_id=payload.get("version_id"),
            version_stage=payload.get("version_stage", "AWSCURRENT"),
        )
    if action == "list_secret_version_ids":
        return {"versions": sm.list_secret_version_ids(payload["name"])}
    if action == "rotate_secret":
        return sm.rotate_secret(
            payload["name"],
            payload["rotation_lambda_arn"],
            payload.get("rotation_rules"),
        )
    if action == "tag_resource":
        sm.tag_resource(payload["name"], payload["tags"])
        return {"tagged": True}
    if action == "untag_resource":
        sm.untag_resource(payload["name"], payload["tag_keys"])
        return {"untagged": True}
    if action == "list_tags_for_resource":
        return {"tags": sm.list_tags_for_resource(payload["name"])}
    raise ValidationError(f"unknown secretsmanager action: {action!r}")


def _dispatch_ssm(app: App, payload: dict):
    action = payload.get("action")
    ss = app.ssm
    if action == "put_parameter":
        return ss.put_parameter(
            payload["name"],
            payload["value"],
            param_type=payload.get("type", "String"),
            description=payload.get("description", ""),
            kms_key_id=payload.get("kms_key_id"),
            overwrite=payload.get("overwrite", False),
            tags=payload.get("tags"),
        )
    if action == "get_parameter":
        return {"parameter": ss.get_parameter(
            payload["name"], payload.get("with_decryption", True)
        )}
    if action == "get_parameters":
        return ss.get_parameters(payload["names"], payload.get("with_decryption", True))
    if action == "get_parameters_by_path":
        return {"parameters": ss.get_parameters_by_path(
            payload["path"],
            recursive=payload.get("recursive", False),
            with_decryption=payload.get("with_decryption", True),
        )}
    if action == "delete_parameter":
        ss.delete_parameter(payload["name"])
        return {"deleted": payload["name"]}
    if action == "delete_parameters":
        return ss.delete_parameters(payload["names"])
    if action == "describe_parameters":
        return {"parameters": ss.describe_parameters(payload.get("filters"))}
    if action == "list_parameter_history":
        return {"history": ss.list_parameter_history(payload["name"])}
    raise ValidationError(f"unknown ssm action: {action!r}")


def _dispatch_cloudwatch(app: App, payload: dict):
    action = payload.get("action")
    cw = app.cloudwatch
    if action == "put_metric_data":
        cw.put_metric_data(payload["namespace"], payload["metric_data"])
        return {"put": True}
    if action == "get_metric_statistics":
        return {"datapoints": cw.get_metric_statistics(
            payload["namespace"],
            payload["metric_name"],
            payload["start_time"],
            payload["end_time"],
            payload.get("period", 60),
            payload.get("statistics"),
            payload.get("dimensions"),
        )}
    if action == "list_metrics":
        return {"metrics": cw.list_metrics(
            payload.get("namespace"), payload.get("metric_name")
        )}
    if action == "create_log_group":
        return cw.create_log_group(payload["log_group_name"], payload.get("kms_key_id"))
    if action == "delete_log_group":
        cw.delete_log_group(payload["log_group_name"])
        return {"deleted": payload["log_group_name"]}
    if action == "list_log_groups":
        return {"log_groups": cw.list_log_groups(payload.get("prefix"))}
    if action == "create_log_stream":
        return cw.create_log_stream(payload["log_group_name"], payload["log_stream_name"])
    if action == "delete_log_stream":
        cw.delete_log_stream(payload["log_group_name"], payload["log_stream_name"])
        return {"deleted": payload["log_stream_name"]}
    if action == "list_log_streams":
        return {"log_streams": cw.list_log_streams(
            payload["log_group_name"], payload.get("prefix")
        )}
    if action == "put_log_events":
        return cw.put_log_events(
            payload["log_group_name"],
            payload["log_stream_name"],
            payload["log_events"],
            payload.get("sequence_token"),
        )
    if action == "get_log_events":
        return cw.get_log_events(
            payload["log_group_name"],
            payload["log_stream_name"],
            payload.get("start_time"),
            payload.get("end_time"),
            payload.get("limit", 100),
        )
    if action == "filter_log_events":
        return cw.filter_log_events(
            payload["log_group_name"],
            payload.get("filter_pattern"),
            payload.get("log_stream_names"),
            payload.get("start_time"),
            payload.get("end_time"),
            payload.get("limit", 100),
        )
    if action == "put_metric_alarm":
        return cw.put_metric_alarm(
            payload["alarm_name"],
            payload["namespace"],
            payload["metric_name"],
            payload["comparison_operator"],
            float(payload["threshold"]),
            payload.get("evaluation_periods", 1),
            payload.get("period", 60),
            payload.get("statistic", "Average"),
            payload.get("description", ""),
            payload.get("alarm_actions"),
            payload.get("ok_actions"),
            payload.get("insufficient_data_actions"),
            payload.get("treat_missing_data", "missing"),
        )
    if action == "describe_alarms":
        return {"alarms": cw.describe_alarms(
            payload.get("alarm_names"), payload.get("state_value")
        )}
    if action == "describe_alarm":
        return cw.describe_alarm(payload["alarm_name"])
    if action == "delete_alarm":
        cw.delete_alarm(payload["alarm_name"])
        return {"deleted": payload["alarm_name"]}
    if action == "set_alarm_state":
        return cw.set_alarm_state(
            payload["alarm_name"],
            payload["state_value"],
            payload.get("state_reason", ""),
        )
    if action == "get_alarm_history":
        return {"history": cw.get_alarm_history(payload["alarm_name"])}
    raise ValidationError(f"unknown cloudwatch action: {action!r}")


def _dispatch_cognito(app: App, payload: dict):
    action = payload.get("action")
    cog = app.cognito
    if action == "create_user_pool":
        return cog.create_user_pool(
            payload["pool_name"],
            payload.get("password_policy"),
            payload.get("auto_verified_attributes"),
            payload.get("username_attributes"),
        )
    if action == "describe_user_pool":
        return cog.describe_user_pool(payload["pool_id"])
    if action == "delete_user_pool":
        cog.delete_user_pool(payload["pool_id"])
        return {"deleted": payload["pool_id"]}
    if action == "list_user_pools":
        return {"user_pools": cog.list_user_pools()}
    if action == "create_user_pool_client":
        return cog.create_user_pool_client(
            payload["pool_id"],
            payload["client_name"],
            payload.get("generate_secret", False),
            payload.get("explicit_auth_flows"),
        )
    if action == "describe_user_pool_client":
        return cog.describe_user_pool_client(payload["pool_id"], payload["client_id"])
    if action == "list_user_pool_clients":
        return {"clients": cog.list_user_pool_clients(payload["pool_id"])}
    if action == "delete_user_pool_client":
        cog.delete_user_pool_client(payload["pool_id"], payload["client_id"])
        return {"deleted": payload["client_id"]}
    if action == "sign_up":
        return cog.sign_up(
            payload["pool_id"],
            payload["client_id"],
            payload["username"],
            payload["password"],
            payload.get("user_attributes"),
        )
    if action == "confirm_sign_up":
        return cog.confirm_sign_up(
            payload["pool_id"],
            payload["client_id"],
            payload["username"],
            payload["confirmation_code"],
        )
    if action == "admin_confirm_sign_up":
        return cog.admin_confirm_sign_up(payload["pool_id"], payload["username"])
    if action == "admin_create_user":
        return cog.admin_create_user(
            payload["pool_id"],
            payload["username"],
            payload["temporary_password"],
            payload.get("user_attributes"),
        )
    if action == "admin_delete_user":
        cog.admin_delete_user(payload["pool_id"], payload["username"])
        return {"deleted": payload["username"]}
    if action == "admin_set_user_password":
        cog.admin_set_user_password(
            payload["pool_id"],
            payload["username"],
            payload["password"],
            payload.get("permanent", True),
        )
        return {"set": True}
    if action == "get_user":
        return cog.get_user(payload["access_token"])
    if action == "admin_get_user":
        return cog.admin_get_user(payload["pool_id"], payload["username"])
    if action == "list_users":
        return {"users": cog.list_users(
            payload["pool_id"], payload.get("filter"), payload.get("limit", 60)
        )}
    if action == "initiate_auth":
        return cog.initiate_auth(
            payload["auth_flow"],
            payload["auth_parameters"],
            payload["client_id"],
            payload.get("pool_id"),
        )
    if action == "global_sign_out":
        return cog.global_sign_out(payload["access_token"])
    if action == "forgot_password":
        return cog.forgot_password(
            payload["pool_id"], payload["client_id"], payload["username"]
        )
    if action == "confirm_forgot_password":
        return cog.confirm_forgot_password(
            payload["pool_id"],
            payload["client_id"],
            payload["username"],
            payload["confirmation_code"],
            payload["new_password"],
        )
    raise ValidationError(f"unknown cognito action: {action!r}")


def make_handler(app: App):
    class Handler(BaseHTTPRequestHandler):
        server_version = f"openaws/{__version__}"

        def log_message(self, *args):  # silence default stderr logging
            pass

        # --- response helpers ------------------------------------------
        def _send_json(self, obj, status=200):
            data = json.dumps(obj).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _send_bytes(self, data, content_type, status=200, headers=None):
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(data)))
            for k, v in (headers or {}).items():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(data)

        def _error(self, exc):
            if isinstance(exc, OpenAWSError):
                self._send_json({"error": exc.code, "message": exc.message}, exc.status)
            else:
                self._send_json({"error": "InternalError", "message": str(exc)}, 500)

        def _read_body(self):
            length = int(self.headers.get("Content-Length", 0) or 0)
            return self.rfile.read(length) if length else b""

        def _read_json(self):
            raw = self._read_body()
            if not raw:
                return {}
            try:
                return json.loads(raw.decode("utf-8"))
            except json.JSONDecodeError as exc:
                raise ValidationError(f"invalid JSON body: {exc}") from exc

        # --- routing ----------------------------------------------------
        def _route(self, method):
            parsed = urlparse(self.path)
            path = parsed.path
            try:
                if path == "/" or path == "/health":
                    return self._send_json(
                        {
                            "service": "openaws",
                            "version": __version__,
                            "services": [
                                "s3", "dynamodb", "sqs", "lambda", "kinesis",
                                "sns", "eventbridge", "stepfunctions",
                                "apigateway", "ses",
                                "iam", "sts", "kms", "secretsmanager",
                                "ssm", "cloudwatch", "cognito",
                            ],
                        }
                    )
                if path.startswith("/s3"):
                    return self._handle_s3(method, path, parsed)
                if path == "/dynamodb":
                    return self._send_json(_dispatch_dynamodb(app, self._read_json()))
                if path == "/sqs":
                    return self._send_json(_dispatch_sqs(app, self._read_json()))
                if path == "/lambda":
                    return self._send_json(_dispatch_lambda(app, self._read_json()))
                if path == "/kinesis":
                    return self._send_json(_dispatch_kinesis(app, self._read_json()))
                if path == "/sns":
                    return self._send_json(_dispatch_sns(app, self._read_json()))
                if path == "/eventbridge":
                    return self._send_json(_dispatch_eventbridge(app, self._read_json()))
                if path == "/stepfunctions":
                    return self._send_json(_dispatch_stepfunctions(app, self._read_json()))
                if path == "/apigateway":
                    return self._send_json(_dispatch_apigateway(app, self._read_json()))
                if path.startswith("/apigw/"):
                    return self._handle_apigw(method, path, parsed)
                if path == "/ses":
                    return self._send_json(_dispatch_ses(app, self._read_json()))
                if path == "/iam":
                    return self._send_json(_dispatch_iam(app, self._read_json()))
                if path == "/sts":
                    return self._send_json(_dispatch_sts(app, self._read_json()))
                if path == "/kms":
                    return self._send_json(_dispatch_kms(app, self._read_json()))
                if path == "/secretsmanager":
                    return self._send_json(_dispatch_secretsmanager(app, self._read_json()))
                if path == "/ssm":
                    return self._send_json(_dispatch_ssm(app, self._read_json()))
                if path == "/cloudwatch":
                    return self._send_json(_dispatch_cloudwatch(app, self._read_json()))
                if path == "/cognito":
                    return self._send_json(_dispatch_cognito(app, self._read_json()))
                return self._send_json({"error": "NotFound", "message": path}, 404)
            except Exception as exc:  # noqa: BLE001 - convert to HTTP error
                return self._error(exc)

        def _handle_apigw(self, method, path, parsed):
            """Route /apigw/<api_id>/<resource_path...> to the gateway."""
            qs = parse_qs(parsed.query, keep_blank_values=True)
            rest = path[len("/apigw/"):].lstrip("/")
            parts = rest.split("/", 1)
            api_id = unquote(parts[0]) if parts else ""
            resource_path = "/" + unquote(parts[1]) if len(parts) > 1 else "/"
            body_bytes = self._read_body()
            body = body_bytes.decode("utf-8") if body_bytes else None
            # flatten single-value query params
            query = {k: v[0] if len(v) == 1 else v for k, v in qs.items()} if qs else None
            headers = dict(self.headers)
            result = app.apigateway.invoke(
                api_id, method, resource_path, body=body,
                query_params=query, headers=headers,
            )
            status = result.get("statusCode", 200)
            resp_headers = result.get("headers", {})
            resp_body = result.get("body", "")
            if isinstance(resp_body, str):
                resp_body = resp_body.encode("utf-8")
            ct = resp_headers.get("Content-Type", "application/json")
            self._send_bytes(resp_body, ct, status=status,
                             headers={k: v for k, v in resp_headers.items()
                                      if k != "Content-Type"})

        def _handle_s3(self, method, path, parsed):
            # /s3                       -> list buckets (GET)
            # /s3/<bucket>              -> create/delete bucket / list objects
            # /s3/<bucket>/<key...>     -> object ops
            qs = parse_qs(parsed.query, keep_blank_values=True)
            rest = path[len("/s3"):].lstrip("/")
            parts = rest.split("/", 1) if rest else []
            if not parts:
                if method == "GET":
                    return self._send_json({"buckets": app.s3.list_buckets()})
                raise ValidationError("unsupported /s3 operation")
            bucket = unquote(parts[0])
            key = unquote(parts[1]) if len(parts) > 1 and parts[1] else None

            # bucket-level operations
            if key is None:
                if method == "PUT":
                    # check for versioning sub-resource: /s3/<bucket>?versioning
                    if "versioning" in qs:
                        body = self._read_json()
                        app.s3.put_bucket_versioning(bucket, body.get("status", "enabled"))
                        return self._send_json({"bucket": bucket, "versioning": body.get("status", "enabled")})
                    return self._send_json(app.s3.create_bucket(bucket), 201)
                if method == "DELETE":
                    app.s3.delete_bucket(bucket)
                    return self._send_json({"deleted": bucket})
                if method == "GET":
                    prefix = qs.get("prefix", [""])[0]
                    delimiter = qs.get("delimiter", [""])[0]
                    if "versions" in qs:
                        return self._send_json(app.s3.list_object_versions(bucket, prefix))
                    if "versioning" in qs:
                        return self._send_json(app.s3.get_bucket_versioning(bucket))
                    result = app.s3.list_objects(bucket, prefix, delimiter)
                    return self._send_json(result)
                raise ValidationError("unsupported bucket operation")

            # object-level operations
            version_id = qs.get("versionId", [None])[0]
            upload_id = qs.get("uploadId", [None])[0]
            part_number_str = qs.get("partNumber", [None])[0]

            if method == "PUT":
                # multipart: initiate
                if "uploads" in qs:
                    ctype = self.headers.get("Content-Type", "application/octet-stream")
                    # collect x-amz-meta-* headers
                    meta = {
                        k[len("x-amz-meta-"):]: v
                        for k, v in self.headers.items()
                        if k.lower().startswith("x-amz-meta-")
                    }
                    return self._send_json(
                        app.s3.create_multipart_upload(bucket, key, ctype, meta or None), 200
                    )
                # multipart: upload part
                if upload_id and part_number_str:
                    body = self._read_body()
                    part_number = int(part_number_str)
                    return self._send_json(
                        app.s3.upload_part(bucket, key, upload_id, part_number, body)
                    )
                # copy from another object
                copy_source = self.headers.get("x-amz-copy-source")
                if copy_source:
                    src = copy_source.lstrip("/")
                    src_bucket, _, src_key = src.partition("/")
                    return self._send_json(app.s3.copy_object(src_bucket, src_key, bucket, key))
                # tagging
                if "tagging" in qs:
                    tags = self._read_json()
                    app.s3.put_object_tagging(bucket, key, tags)
                    return self._send_json({"tagged": key})
                # regular put
                body = self._read_body()
                ctype = self.headers.get("Content-Type", "application/octet-stream")
                meta = {
                    k[len("x-amz-meta-"):]: v
                    for k, v in self.headers.items()
                    if k.lower().startswith("x-amz-meta-")
                }
                return self._send_json(
                    app.s3.put_object(bucket, key, body, ctype, meta or None), 201
                )

            if method == "POST":
                # multipart: complete
                if upload_id:
                    body = self._read_json()
                    parts = body.get("parts", [])
                    return self._send_json(
                        app.s3.complete_multipart_upload(bucket, key, upload_id, parts)
                    )
                # presigned URL generation
                if "presign" in qs:
                    expires_in = int(qs.get("expires_in", ["3600"])[0])
                    operation = qs.get("operation", ["get_object"])[0]
                    return self._send_json(
                        app.s3.generate_presigned_url(bucket, key, operation, expires_in)
                    )
                raise ValidationError("unsupported POST on object")

            if method == "GET":
                # list parts for an in-progress multipart
                if upload_id:
                    return self._send_json(
                        {"parts": app.s3.list_parts(bucket, key, upload_id)}
                    )
                if "tagging" in qs:
                    return self._send_json({"tags": app.s3.get_object_tagging(bucket, key)})
                obj = app.s3.get_object(bucket, key, version_id)
                extra_headers = {"ETag": obj["etag"]}
                if obj.get("version_id"):
                    extra_headers["x-amz-version-id"] = obj["version_id"]
                return self._send_bytes(
                    obj["body"], obj["content_type"], headers=extra_headers
                )

            if method == "DELETE":
                # multipart: abort
                if upload_id:
                    app.s3.abort_multipart_upload(bucket, key, upload_id)
                    return self._send_json({"aborted": upload_id})
                if "tagging" in qs:
                    app.s3.delete_object_tagging(bucket, key)
                    return self._send_json({"untagged": key})
                app.s3.delete_object(bucket, key, version_id)
                return self._send_json({"deleted": key})

            raise ValidationError("unsupported object operation")

        def do_GET(self):
            self._route("GET")

        def do_PUT(self):
            self._route("PUT")

        def do_POST(self):
            self._route("POST")

        def do_DELETE(self):
            self._route("DELETE")

    return Handler


class OpenAWSServer:
    """A ThreadingHTTPServer wrapper that can run in-process for tests."""

    def __init__(self, host: str = "127.0.0.1", port: int = 0, data_dir: str | None = None,
                 app: App | None = None):
        self.app = app or App(data_dir)
        self._httpd = ThreadingHTTPServer((host, port), make_handler(self.app))
        self._thread: threading.Thread | None = None

    @property
    def host(self) -> str:
        return self._httpd.server_address[0]

    @property
    def port(self) -> int:
        return self._httpd.server_address[1]

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    def start(self) -> "OpenAWSServer":
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)
        self._thread.start()
        return self

    def serve_forever(self):  # pragma: no cover - blocking entry for CLI
        self._httpd.serve_forever()

    def stop(self):
        self._httpd.shutdown()
        self._httpd.server_close()
        if self._thread:
            self._thread.join(timeout=2)
