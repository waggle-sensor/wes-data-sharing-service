import argparse
import logging
import json
import pika
import wagglemsg

from contextlib import ExitStack
from os import getenv
from functools import lru_cache
from prometheus_client import start_http_server, Counter
from redis import Redis


wes_data_service_messages_total = Counter("wes_data_service_messages_total", "Total number of messages handled.", [])
wes_data_service_messages_rejected_total = Counter("wes_data_service_messages_rejected_total", "Total number of invalid messages.")
wes_data_service_messages_published_node_total = Counter("wes_data_service_messages_published_node_total", "Total number of messages published to node.")
wes_data_service_messages_published_beehive_total = Counter("wes_data_service_messages_published_beehive_total", "Total number of messages published to beehive.")


class InvalidMessageError(Exception):
    def __init__(self, error):
        self.error = error


class AppMetaCache:

    def __init__(self, host="wes-app-meta-cache"):
        self.client = Redis(host)

    @lru_cache(maxsize=128)
    def get(self, app_uid):
        key = f"app-meta.{app_uid}"
        data = self.client.get(key)
        if data is None:
            return None
        return json.loads(data)


class MessageHandler:

    logger = logging.getLogger("MessageHandler")

    def __init__(self, upload_publish_name="upload", system_meta={}):
        self.upload_publish_name = upload_publish_name
        self.system_meta = system_meta
        self.app_meta_cache = AppMetaCache("wes-app-meta-cache")

    def on_message_callback(self, ch, method, properties, body):
        self.logger.debug("handling delivery...")
        wes_data_service_messages_total.inc()

        app_uid = properties.app_id

        if app_uid is None:
            self.logger.info("reject msg: no pod uid: %r", body)
            ch.basic_reject(method.delivery_tag, False)
            wes_data_service_messages_rejected_total.inc()
            return

        try:
            msg = wagglemsg.load(body)
        except Exception:
            self.logger.info("reject msg: bad data: %r", body)
            ch.basic_reject(method.delivery_tag, False)
            wes_data_service_messages_rejected_total.inc()
            return

        # handle upload message case: needs to have value changed to url
        if msg.name == "upload":
            msg = convert_to_upload_message(msg, self.upload_publish_name)

        # update message app meta
        app_meta = self.app_meta_cache.get(app_uid)
        
        if app_meta is None:
            self.logger.info("reject msg: no pod meta: %r", msg)
            ch.basic_reject(method.delivery_tag, False)
            wes_data_service_messages_rejected_total.inc()
            return

        for k, v in app_meta.items():
            msg.meta[k] = v

        # update message system meta
        for k, v in self.system_meta.items():
            msg.meta[k] = v

        self.logger.info("msg: %r", msg)

        self.route_message(ch, method.routing_key, msg)
        ch.basic_ack(method.delivery_tag)
    
    def route_message(self, ch, routing_key: str, msg: wagglemsg.Message):
        body = wagglemsg.dump(msg)

        if routing_key in ["node", "all"]:
            self.logger.debug("forwarding message type %r to node", msg.name)
            ch.basic_publish("data.topic", msg.name, body)
            wes_data_service_messages_published_node_total.inc()

        if routing_key in ["beehive", "all"]:
            self.logger.debug("forwarding message type %r to beehive", msg.name)
            ch.basic_publish("to-beehive", msg.name, body)
            wes_data_service_messages_published_beehive_total.inc()


def convert_to_upload_message(msg: wagglemsg.Message, upload_publish_name: str) -> wagglemsg.Message:
    return wagglemsg.Message(
        timestamp=msg.timestamp,
        name=upload_publish_name,
        meta=msg.meta,  # TODO(sean) be careful on ownership here, in case this is mutated
        value=upload_url_for_message(msg),
    )


def upload_url_for_message(msg: wagglemsg.Message) -> str:
    try:
        job = msg.meta["job"]
        task = msg.meta["task"]
        node = msg.meta["node"]
        filename = msg.meta["filename"]
        plugin = msg.meta["plugin"]
        namespace = "sage"
        tag = plugin.split(":")[-1]
    except KeyError as exc:
        raise InvalidMessageError(f"message missing fields for upload url: {exc}")
    return f"https://storage.sagecontinuum.org/api/v1/data/{job}/{namespace}-{task}-{tag}/{node}/{msg.timestamp}-{filename}"


def declare_exchange_with_queue(ch: pika.adapters.blocking_connection.BlockingChannel, name: str):
    ch.exchange_declare(name, exchange_type="fanout", durable=True)
    ch.queue_declare(name, durable=True)
    ch.queue_bind(name, name)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--debug",
        action="store_true",
        help="enable verbose logging",
    )
    parser.add_argument(
        "--upload-publish-name",
        default=getenv("UPLOAD_PUBLISH_NAME", "upload"),
        help="measurement name to publish uploads to",
    )
    parser.add_argument(
        "--kubeconfig",
        default=None,
        help="kubernetes config",
    )
    parser.add_argument(
        "--rabbitmq-host",
        default=getenv("RABBITMQ_HOST", "rabbitmq-server"),
        help="rabbitmq host",
    )
    parser.add_argument(
        "--rabbitmq-port",
        default=int(getenv("RABBITMQ_PORT", "5672")),
        type=int,
        help="rabbitmq port",
    )
    parser.add_argument(
        "--rabbitmq-username",
        default=getenv("RABBITMQ_USERNAME", "service"),
        help="rabbitmq username",
    )
    parser.add_argument(
        "--rabbitmq-password",
        default=getenv("RABBITMQ_PASSWORD", "service"),
        help="rabbitmq password",
    )
    parser.add_argument(
        "--waggle-node-id",
        default=getenv("WAGGLE_NODE_ID", "0000000000000000"),
        help="waggle node id",
    )
    parser.add_argument(
        "--waggle-node-vsn",
        default=getenv("WAGGLE_NODE_VSN", "W000"),
        help="waggle node vsn",
    )
    parser.add_argument(
        "--metrics-port",
        default=getenv("METRICS_PORT", "8080"),
        type=int,
        help="metrics server port",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(message)s",
        datefmt="%Y/%m/%d %H:%M:%S",
    )
    # turn down the super verbose library level logging
    logging.getLogger("pika").setLevel(logging.CRITICAL)

    params = pika.ConnectionParameters(
        host=args.rabbitmq_host,
        port=args.rabbitmq_port,
        credentials=pika.PlainCredentials(
            username=args.rabbitmq_username,
            password=args.rabbitmq_password,
        ),
        client_properties={"name": "data-sharing-service"},
        connection_attempts=3,
        retry_delay=10,
        heartbeat=900,
        blocked_connection_timeout=600,
    )

    logging.info("will publish uploads under name %r", args.upload_publish_name)
    # publisher = Publisher(params)

    # start metrics server
    start_http_server(args.metrics_port)

    message_handler = MessageHandler(
        system_meta={
            "node": args.waggle_node_id,
            "vsn": args.waggle_node_vsn,
        },
    )

    with ExitStack() as es:
        logging.info("connecting consumer to rabbitmq server at %s:%d as %s.",
            params.host,
            params.port,
            params.credentials.username,
        )

        connection = es.enter_context(pika.BlockingConnection(params))
        channel = es.enter_context(connection.channel())

        logging.info("setting up queues and exchanges.")
        channel.exchange_declare("data.topic", exchange_type="topic", durable=True)
        declare_exchange_with_queue(channel, "to-validator")
        declare_exchange_with_queue(channel, "to-beehive")

        logging.info("starting consumer.")
        channel.basic_consume("to-validator", message_handler.on_message_callback, auto_ack=False)
        channel.start_consuming()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
