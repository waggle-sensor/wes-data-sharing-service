import argparse
import logging
from os import getenv
import time
import json
from dataclasses import dataclass
from contextlib import ExitStack
from functools import lru_cache
from typing import Deque
from prometheus_client import start_http_server, Counter, Gauge
import redis
import pika
import wagglemsg


wes_data_service_messages_handled_total = Counter("wes_data_service_messages_handled_total", "Total number of messages handled.")
wes_data_service_messages_invalid_total = Counter("wes_data_service_messages_invalid_total", "Total number of invalid messages.")
wes_data_service_messages_backlogged_total = Counter("wes_data_service_messages_backlogged_total", "Total number of messages which have been backlogged.")
wes_data_service_messages_published_total = Counter("wes_data_service_messages_published_total", "Total number of messages published.")
wes_data_service_messages_expired_total = Counter("wes_data_service_messages_expired_total", "Total number of messages expired.")
wes_data_service_pods_expired_total = Counter("wes_data_service_pods_expired_total", "Total number of pods expired.")
wes_data_service_messages_in_backlog = Gauge("wes_data_service_messages_in_backlog", "Number of messages currently in backlog.")
wes_data_service_pods_in_backlog = Gauge("wes_data_service_pods_in_backlog", "Number of pods currently in backlog.")


@dataclass
class Delivery:

    channel: pika.adapters.blocking_connection.BlockingChannel = None
    delivery_tag: int = 0
    routing_key: str = None
    pod_uid: str = None
    body: bytes = None

    def ack(self):
        self.channel.basic_ack(self.delivery_tag)

    def reject(self):
        self.channel.basic_reject(self.delivery_tag)

    def __str__(self):
        return str(self.delivery_tag)


def consume_deliveries(channel, queue, handler):
    def pika_handler(ch, method, properties, body):
        handler(Delivery(
            channel=ch,
            delivery_tag=method.delivery_tag,
            routing_key=method.routing_key,
            pod_uid=properties.app_id,
            body=body,
        ))
    channel.basic_consume(queue, pika_handler)


@dataclass
class Publishing:

    body: bytes
    properties: pika.BasicProperties = None


class Publisher:

    def __init__(self, channel):
        self.channel = channel

    def publish(self, exchange, routing_key, publishing):
        self.channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            properties=publishing.properties,
            body=publishing.body,
        )


class PodCache:

    def __init__(self):
        self.client = redis.Redis()
    
    @lru_cache(maxsize=1024)
    def get(self, key):
        r = self.client.get(key)
        if r is None:
            raise KeyError(key)
        return json.loads(r)


class InvalidMessageError(Exception):
    """InvalidMessageError is throw to indicate that a message is invalid. (Bad JSON data, missing fields, etc.)"""

    def __init__(self, error):
        self.error = error


class MonotonicClock:

    def now(self):
        return time.monotonic()


@dataclass
class MessageHandlerConfig:
    node: str
    vsn: str
    upload_publish_name: str
    pod_state_expire_duration: float = 7200.0
    pod_without_metadata_state_expire_duration: float = 300.0


@dataclass
class BacklogItem:
    delivery: Delivery
    received_at: float


class MessageHandler:
    """
    MessageHandler handles pod and message events and publishes messages triggered by these.

    This implementation is designed to have the following behavior:

    1. Messages with known pod metadata are immediately published.
    2. Messages with unknown pod metadata are added to a backlog for the message's pod UID.
    3. When a pod event is handled, the backlog for that Pod UID is immediately flushed.
    4. Pod metadata expires after config.pod_state_expire_duration seconds. Any pod or
       message events reset the expiration time for the message's pod UID. When a pod
       expires all messages in the backlog are dropped.
    """

    logger = logging.getLogger("MessageHandler")

    def __init__(self, config: MessageHandlerConfig, publisher, pod_cache: PodCache, clock=MonotonicClock()):
        self.config = config
        self.publisher = publisher
        self.clock = clock
        self.pod_cache: PodCache = pod_cache
        self.backlog = Deque[BacklogItem]()

    def handle_delivery(self, delivery: Delivery):
        self.logger.debug("handling delivery...")
        wes_data_service_messages_handled_total.inc()

        if delivery.pod_uid is None:
            self.logger.debug("dropping message without pod uid")
            wes_data_service_messages_invalid_total.inc()
            delivery.reject()
            return
        
        if self.pod_cache[delivery.pod_uid] is None:
            self.logger.debug("adding delivery %s to backlog for %s", delivery, delivery.pod_uid)
            wes_data_service_messages_backlogged_total.inc()
            wes_data_service_messages_in_backlog.inc()
            self.backlog.append(delivery)
        else:
            self.load_and_publish_message(delivery)

    def prune_backlog(self):
        self.logger.info("pruning backlog with %d items...", len(self.backlog))

        for _ in range(len(self.backlog)):
            item = self.backlog.popleft()

            # if item has pod info, publish now
            try:
                # check right errors here
                self.pod_cache[item.delivery.pod_uid]
                self.load_and_publish_message(item.delivery)
                continue
            except KeyError:
                pass
            except InvalidMessageError:
                item.delivery.reject()
                continue

            # TODO(sean) stand up kubernetes test dev object with all these available

            # if item has expired, drop it
            if self.clock.now() - item.received_at > 30.0:
                item.delivery.reject()
                continue

            # otherwise, return the item to the backlog
            self.backlog.append(item)

        self.logger.info("pruned backlog to %d items", len((self.backlog)))
        # set backlog size metric here...

    def load_and_publish_message(self, delivery: Delivery):
        self.logger.debug("publishing message %s...",  delivery)

        try:
            msg = wagglemsg.load(delivery.body)
        except Exception:
            self.logger.debug("failed to load waggle message: %s", delivery.body)
            delivery.reject()
            return

            update_message_with_config_metadata(msg, self.config)
            update_message_with_pod_metadata(msg, self.pod_state[delivery.pod_uid].pod)
        except Exception:
            if self.logger.isEnabledFor(logging.DEBUG):
                self.logger.exception("failed to load waggle message")
            delivery.ack()
            wes_data_service_messages_invalid_total.inc()
            return
        
        if msg.name == "upload":
            msg = convert_to_upload_message(msg, self.config)

        body = wagglemsg.dump(msg)

        if delivery.routing_key in ["node", "all"]:
            self.logger.debug("forwarding message type %r to local", msg.name)
            self.publisher.publish("data.topic", msg.name, Publishing(body))

        if delivery.routing_key in ["beehive", "all"]:
            self.logger.debug("forwarding message type %r to beehive", msg.name)
            self.publisher.publish("to-beehive", msg.name, Publishing(body, pika.BasicProperties(delivery_mode=2)))

        delivery.ack()
        self.logger.debug("published message %s",  delivery)
        wes_data_service_messages_published_total.inc()


def update_message_with_config_metadata(msg: wagglemsg.Message, config: MessageHandlerConfig):
    msg.meta["node"] = config.node
    msg.meta["vsn"] = config.vsn


def update_message_with_pod_metadata(msg: wagglemsg.Message, pod):
    # add scheduler metadata using pod uid
    msg.meta["host"] = pod.host
    # TODO include namespace
    msg.meta["plugin"] = pod.image.split("/")[-1]
    msg.meta["job"] = pod.labels.get("sagecontinuum.org/plugin-job", "sage")

    try:
        msg.meta["task"] = pod.labels["sagecontinuum.org/plugin-task"]
    except KeyError:
        raise InvalidMessageError(f"pod {pod.name} missing task label")


def convert_to_upload_message(msg: wagglemsg.Message, config: MessageHandlerConfig) -> wagglemsg.Message:
    return wagglemsg.Message(
        timestamp=msg.timestamp,
        name=config.upload_publish_name,
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


def call_every(connection, interval, func):
    def func_every():
        func()
        connection.call_later(interval, func_every)
    connection.call_later(interval, func_every)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true", help="enable verbose logging")
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
    parser.add_argument(
        "--pod-expire-duration",
        type=float,
        default=7200.0,
        help="expiration time for pods in seconds",
    )
    parser.add_argument(
        "--pod-without-metadata-expire-duration",
        type=float,
        default=300.0,
        help="expiration time for pods without metadata in seconds",
    )
    args = parser.parse_args()

    # config should never be this way but this is an explicit sanity check
    assert args.pod_without_metadata_expire_duration <= args.pod_expire_duration

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
    publisher = Publisher(params)

    # start metrics server
    start_http_server(args.metrics_port)

    handler = MessageHandler(
        config=MessageHandlerConfig(
            node=args.waggle_node_id,
            vsn=args.waggle_node_vsn,
            upload_publish_name=args.upload_publish_name,
            pod_state_expire_duration=args.pod_expire_duration,
            pod_without_metadata_state_expire_duration=args.pod_without_metadata_expire_duration,
        ),
        publisher=publisher,
    )

    while True:
        with ExitStack() as es:
            logging.info(
                "connecting consumer to rabbitmq server at %s:%d as %s.",
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

            def forward_pod_events():
                logging.debug("updating pod events...")
                for pod in pod_event_watcher.ready_events():
                    if pod is None:
                        raise RuntimeError("pod watcher terminated")
                    handler.handle_pod(pod)
                logging.debug("updated pod events")

            call_every(connection, 1.0, forward_pod_events)
            call_every(connection, 10.0, handler.handle_expired_pods)
            consume_deliveries(channel, "to-validator", handler.handle_delivery)

            logging.info("starting consumer...")
            try:
                channel.start_consuming()
            except KeyboardInterrupt:
                return
            except Exception:
                logging.exception("consumer disconnected. will retry...")

        # # TODO(sean) figure out clean way of handling this. when the channel expires, I assume the
        # outstanding deliveries must be consumed again.
        logging.info("clearing backlogged messages but keeping pod state")
        handler.clear_backlogs_but_keep_pod_state()
        time.sleep(3.0)

        # we can just allow the cache to expire stuff separately...


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
