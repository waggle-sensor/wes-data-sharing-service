import argparse
import logging
import json
import pika
import prometheus_client
import threading
import wagglemsg

from contextlib import ExitStack
from os import getenv
from functools import lru_cache
from prometheus_client import Counter
from redis import Redis

SCOPE_ALL = "all"
SCOPE_NODE = "node"
SCOPE_BEEHIVE = "beehive"


class InvalidMessageError(Exception):
    def __init__(self, error):
        self.error = error


class AppMetaCache:

    def __init__(self, host, port):
        self.client = Redis(host=host, port=port)

    @lru_cache(maxsize=128)
    def __getitem__(self, app_uid):
        key = f"app-meta.{app_uid}"
        data = self.client.get(key)
        if data is None:
            raise KeyError(app_uid)
        return json.loads(data)


class MetricServer:

    def __init__(self, host, port, registry):
        from wsgiref.simple_server import make_server, WSGIRequestHandler

        class SilentHandler(WSGIRequestHandler):
            def log_message(self, format, *args):
                pass

        app = prometheus_client.make_wsgi_app(registry)
        self.httpd = make_server(host, port, app, handler_class=SilentHandler)

    def run(self):
        self.httpd.serve_forever()

    def shutdown(self):
        try:
            self.httpd.shutdown()
        finally:
            self.httpd.server_close()


class Service:

    logger = logging.getLogger("Service")

    def __init__(
        self,
        connection_parameters,
        src_queue,
        dst_exchange_beehive,
        dst_exchange_node,
        metrics_host,
        metrics_port,
        upload_publish_name,
        system_meta,
        app_meta_cache,
        system_users,
        ):

        self.connection_parameters = connection_parameters
        self.src_queue = src_queue
        self.dst_exchange_beehive = dst_exchange_beehive
        self.dst_exchange_node = dst_exchange_node
        self.connection = None

        self.metrics_host = metrics_host
        self.metrics_port = metrics_port

        self.upload_publish_name = upload_publish_name
        self.system_meta = system_meta
        self.app_meta_cache = app_meta_cache
        self.system_users = system_users

        self.connected = threading.Event()
        self.stopped = threading.Event()
        self.stopped.set()

    def shutdown(self):
        self.connected.wait()
        self.connection.add_callback_threadsafe(self.connection.close)
        self.stopped.wait()

    def run(self):
        with ExitStack() as es:
            self.stopped.clear()
            es.callback(self.stopped.set)

            self.logger.info("connecting consumer to rabbitmq server at %s:%d as %s.",
                self.connection_parameters.host,
                self.connection_parameters.port,
                self.connection_parameters.credentials.username,
            )
            self.connection = es.enter_context(pika.BlockingConnection(self.connection_parameters))
            self.channel = es.enter_context(self.connection.channel())
            self.connected.set()

            self.logger.info("setting up queues and exchanges.")
            declare_exchange_with_queue(self.channel, self.src_queue)
            declare_exchange_with_queue(self.channel, self.dst_exchange_beehive)
            self.channel.exchange_declare(self.dst_exchange_node, exchange_type="topic", durable=True)

            # register and run fresh set of metrics and metrics server
            self.logger.info("starting metric server on %s:%d.", self.metrics_host, self.metrics_port)
            registry = prometheus_client.CollectorRegistry()
            self.messages_total = Counter("wes_data_service_messages_total", "Total number of messages handled.", registry=registry)
            self.messages_rejected_total = Counter("wes_data_service_messages_rejected_total", "Total number of invalid messages.", registry=registry)
            self.messages_published_node_total = Counter("wes_data_service_messages_published_node_total", "Total number of messages published to node.", registry=registry)
            self.messages_published_beehive_total = Counter("wes_data_service_messages_published_beehive_total", "Total number of messages published to beehive.", registry=registry)
            metrics_server = MetricServer(self.metrics_host, self.metrics_port, registry)
            threading.Thread(target=metrics_server.run, daemon=True).start()
            es.callback(metrics_server.shutdown)

            self.logger.info("starting consumer on %s.", self.src_queue)
            self.channel.basic_consume(self.src_queue, self.on_message_callback, auto_ack=False)
            self.channel.start_consuming()

    def on_message_callback(self, ch, method, properties, body):
        self.logger.debug("handling delivery...")
        self.messages_total.inc()

        app_uid = properties.app_id

        if app_uid is None and properties.user_id is None:
            self.logger.warning("reject msg: no pod uid: %r", body)
            ch.basic_reject(method.delivery_tag, False)
            self.messages_rejected_total.inc()
            return

        try:
            msg = wagglemsg.load(body)
        except Exception:
            self.logger.warning("reject msg: bad data: %r", body)
            ch.basic_reject(method.delivery_tag, False)
            self.messages_rejected_total.inc()
            return

        # update app meta using app meta cache if not system user
        if properties.user_id not in self.system_users:
            try:
                app_meta = self.app_meta_cache[app_uid]
            except KeyError:
                self.logger.warning("reject msg: no app meta: %r %r", app_uid, msg)
                ch.basic_reject(method.delivery_tag, False)
                self.messages_rejected_total.inc()
                return

            msg.meta.update(app_meta)

        # update system metadata
        msg.meta.update(self.system_meta)

        # handle upload message case: needs to have value changed to url
        if msg.name == self.upload_publish_name:
            try:
                msg = convert_to_upload_message(msg, self.upload_publish_name)
            except InvalidMessageError:
                self.logger.warning("reject msg: bad upload message: %r", msg)
                ch.basic_reject(method.delivery_tag, False)
                self.messages_rejected_total.inc()
                return

        self.publish_message(ch, method.routing_key, msg)
        ch.basic_ack(method.delivery_tag)

    def publish_message(self, ch, routing_key: str, msg: wagglemsg.Message):
        body = wagglemsg.dump(msg)

        if routing_key in [SCOPE_NODE, SCOPE_ALL]:
            self.logger.debug("publishing message %r to node", msg)
            properties = pika.BasicProperties(delivery_mode=pika.DeliveryMode.Transient)
            ch.basic_publish("data.topic", msg.name, body, properties=properties)
            self.messages_published_node_total.inc()

        if routing_key in [SCOPE_BEEHIVE, SCOPE_ALL]:
            self.logger.debug("publishing message %r to beehive", msg)
            properties = pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent)
            ch.basic_publish("to-beehive", msg.name, body, properties=properties)
            self.messages_published_beehive_total.inc()


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
    except KeyError as exc:
        raise InvalidMessageError(f"message missing fields for upload url: {exc}")

    plugin_name = plugin.split("/")[-1]
    plugin_name_parts = plugin_name.split(":")

    if len(plugin_name_parts) == 1:
        tag = "latest"
    elif len(plugin_name_parts) == 2:
        tag = plugin_name_parts[-1]
    else:
        raise InvalidMessageError(f"invalid plugin image name: {plugin!r}")

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
        "--rabbitmq-host",
        default=getenv("RABBITMQ_HOST", "wes-rabbitmq"),
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
        "--app-meta-cache-host",
        default=getenv("APP_META_CACHE_HOST", "wes-app-meta-cache"),
        help="app meta cache host",
    )
    parser.add_argument(
        "--app-meta-cache-port",
        default=getenv("APP_META_CACHE_PORT", "6379"),
        type=int,
        help="app meta cache port",
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
        "--metrics-host",
        default=getenv("METRICS_HOST", "0.0.0.0"),
        help="metrics server host",
    )
    parser.add_argument(
        "--metrics-port",
        default=getenv("METRICS_PORT", "8080"),
        type=int,
        help="metrics server port",
    )
    parser.add_argument(
        "--src-queue",
        default=getenv("SRC_QUEUE", "to-validator"),
        help="source queue to process",
    )
    parser.add_argument(
        "--dst-exchange-beehive",
        default=getenv("DST_EXCHANGE_BEEHIVE", "to-beehive"),
        help="destination exchange for beehive",
    )
    parser.add_argument(
        "--dst-exchange-node",
        default=getenv("DST_EXCHANGE_NODE", "data.topic"),
        help="destination exchange for node",
    )
    parser.add_argument(
        "--system-users",
        default=getenv("SYSTEM_USERS", ""),
        help="space separated list of system users. system users do not update their meta against the app meta cache",
    )
    args = parser.parse_args()

    system_users = args.system_users.split()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(message)s",
        datefmt="%Y/%m/%d %H:%M:%S",
    )
    # turn down the super verbose library level logging
    logging.getLogger("pika").setLevel(logging.CRITICAL)

    service = Service(
        # rabbitmq config
        connection_parameters=pika.ConnectionParameters(
            host=args.rabbitmq_host,
            port=args.rabbitmq_port,
            credentials=pika.PlainCredentials(
                username=args.rabbitmq_username,
                password=args.rabbitmq_password,
            ),
            client_properties={"name": "wes-data-sharing-service"},
            connection_attempts=3,
            retry_delay=10,
        ),
        src_queue=args.src_queue,
        dst_exchange_beehive=args.dst_exchange_beehive,
        dst_exchange_node=args.dst_exchange_node,

        # metrics config
        metrics_host=args.metrics_host,
        metrics_port=args.metrics_port,

        # app meta cache config
        app_meta_cache=AppMetaCache(
            host=args.app_meta_cache_host,
            port=args.app_meta_cache_port,
        ),

        # service specific config
        upload_publish_name=args.upload_publish_name,
        system_meta={
            "node": args.waggle_node_id,
            "vsn": args.waggle_node_vsn,
        },
        system_users=system_users,
    )

    service.run()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
