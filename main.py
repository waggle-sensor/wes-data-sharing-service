import argparse
import logging
from os import getenv
import kubernetes
import pika
import wagglemsg
from pathlib import Path
import time
import time
from dataclasses import dataclass
from pod_event_watcher import PluginPodEventWatcher
import amqp
from prometheus_client import start_http_server, Counter


wes_data_service_messages_handled_total = Counter("wes_data_service_messages_handled_total", "Total number of messages handled.")
wes_data_service_messages_invalid_total = Counter("wes_data_service_messages_invalid_total", "Total number of invalid messages.")
wes_data_service_messages_backlogged_total = Counter("wes_data_service_messages_backlogged_total", "Total number of messages which have been backlogged.")
wes_data_service_messages_published_total = Counter("wes_data_service_messages_published_total", "Total number of messages published.")
wes_data_service_messages_expired_total = Counter("wes_data_service_messages_expired_total", "Total number of messages expired.")


class InvalidMessageError(Exception):
    """InvalidMessageError is throw to indicate that a message is invalid. (Bad JSON data, missing fields, etc.)"""

    def __init__(self, error):
        self.error = error


@dataclass
class PodState:

    pod: object
    backlog: list
    updated_at: float


@dataclass
class MessageHandlerConfig:
    node: str
    vsn: str
    upload_publish_name: str
    update_pod_events_interval: float = 1.0
    update_pod_state_interval: float = 10.0
    pod_state_expire_duration: float = 30.0


class MessageHandler:

    # TODO(sean) refactor and use a driver to test behavior on pod events and messages.

    def __init__(self, config: MessageHandlerConfig, connection: pika.BlockingConnection, channel: pika.adapters.blocking_connection.BlockingChannel,
        pod_event_watcher: PluginPodEventWatcher):
        self.config = config
        self.connection = connection
        self.channel = channel
        self.pod_event_watcher = pod_event_watcher
        self.pod_state = {}
        self.call_every(config.update_pod_events_interval, self.update_pod_events)
        self.call_every(config.update_pod_state_interval, self.update_pod_state)

    def call_every(self, interval, func):
        def func_every():
            func()
            self.connection.call_later(interval, func_every)
        self.connection.call_later(interval, func_every)

    def handle(self, delivery: amqp.Delivery):
        wes_data_service_messages_handled_total.inc()

        logging.debug("handling delivery...")
        pod_uid = delivery.properties.app_id
        
        if pod_uid is None:
            wes_data_service_messages_invalid_total.inc()
            logging.debug("dropping message without pod uid")
            delivery.ack()
            return

        # add placeholder for pod state for unknown pod uid
        if pod_uid not in self.pod_state:
            logging.debug("adding pod state placeholder for %s", pod_uid)
            self.pod_state[pod_uid] = PodState(pod=None, backlog=[], updated_at=time.monotonic())
        
        # add messages to backlog for unknown pod
        if self.pod_state[pod_uid].pod is None:
            wes_data_service_messages_backlog_total.inc()
            logging.debug("adding delivery %s to backlog for %s", delivery, pod_uid)
            pod_state = self.pod_state[pod_uid]
            pod_state.backlog.append(delivery)
            pod_state.updated_at = time.monotonic()
            return

        # handle delivery right away for known pod
        self.pod_state[pod_uid].updated_at = time.monotonic()
        self.load_and_publish_message(delivery)

    def update_pod_events(self):
        logging.debug("updating pod events...")

        # TODO(sean) think about the right kind of failure mode when the pod event watcher stops.
        if self.pod_event_watcher.is_stopped():
            raise RuntimeError("pod event watcher has stopped")

        for pod in self.pod_event_watcher.ready_events():
            pod_uid = pod.metadata.uid
            logging.debug("received pod event %s (%s)", pod.metadata.name, pod_uid)

            if pod_uid not in self.pod_state:
                logging.debug("added pod state for %s (%s)", pod_uid, pod.metadata.name)
                self.pod_state[pod_uid] = PodState(pod=pod, backlog=[], updated_at=time.monotonic())
            else:
                logging.debug("updated placeholder for %s (%s)", pod_uid, pod.metadata.name)
                pod_state = self.pod_state[pod_uid]
                pod_state.pod = pod
                pod_state.updated_at = time.monotonic()

            self.flush_pod_backlog(pod_uid)
        logging.debug("updated pod events")

    def update_pod_state(self):
        logging.debug("updating pod state...")
        for pod_uid in list(self.pod_state.keys()):
            pod_state = self.pod_state[pod_uid]
            self.flush_pod_backlog(pod_uid)

            if time.monotonic() - pod_state.updated_at < self.config.pod_state_expire_duration:
                continue
            
            logging.debug("expiring pod state for %s", pod_uid)
            for delivery in pod_state.backlog:
                delivery.ack()
                wes_data_service_messages_expired_total.inc()

            del self.pod_state[pod_uid]

        logging.debug("updated pod state")

    def flush_pod_backlog(self, pod_uid):
        logging.debug("flushing pod backlog for %s...", pod_uid)
        pod_state = self.pod_state[pod_uid]
        if pod_state.pod is None:
            return
        for delivery in pod_state.backlog:
            self.load_and_publish_message(delivery)
        pod_state.backlog.clear()
        logging.debug("flushed pod backlog")

    def load_and_publish_message(self, delivery: amqp.Delivery):
        logging.debug("publishing message %s...",  delivery)
        try:
            msg = self.load_message(delivery)
            self.publish_message(delivery.method.routing_key, msg)
        except InvalidMessageError as e:
            delivery.ack()
            logging.error("invalid message: %s", e)
            wes_data_service_messages_invalid_total.inc()
            return

        delivery.ack()
        logging.debug("published message %s",  delivery)
        wes_data_service_messages_published_total.inc()

    def load_message(self, delivery: amqp.Delivery):
        try:
            msg = wagglemsg.load(delivery.body)
        except Exception:
            raise InvalidMessageError(f"failed to parse message body {delivery.body!r}")

        update_message_with_config_metadata(msg, self.config)
        update_message_with_pod_metadata(msg, self.pod_state[delivery.properties.app_id].pod)

        if msg.name == "upload":
            return convert_to_upload_message(msg, self.config)

        return msg

    def publish_message(self, scope: str, msg: wagglemsg.Message):
        body = wagglemsg.dump(msg)

        if scope not in {"node", "beehive", "all"}:
            raise InvalidMessageError(f"invalid message scope {scope!r}")

        if scope in {"node", "all"}:
            logging.debug("forwarding message type %r to local", msg.name)
            self.channel.basic_publish(exchange="data.topic", routing_key=msg.name, body=body)

        if scope in {"beehive", "all"}:
            logging.debug("forwarding message type %r to beehive", msg.name)
            # messages to beehive are always marked as persistent
            properties = pika.BasicProperties(delivery_mode=2)
            self.channel.basic_publish(
                exchange="to-beehive",
                routing_key=msg.name,
                properties=properties,
                body=body,
            )


def update_message_with_config_metadata(msg: wagglemsg.Message, config: MessageHandlerConfig):
    msg.meta["node"] = config.node
    msg.meta["vsn"] = config.vsn


def update_message_with_pod_metadata(msg: wagglemsg.Message, pod):
    # add scheduler metadata using pod uid
    msg.meta["host"] = pod.spec.node_name
    # TODO include namespace
    msg.meta["plugin"] = pod.spec.containers[0].image.split("/")[-1]
    msg.meta["job"] = pod.metadata.labels.get("sagecontinuum.org/plugin-job", "sage")

    try:
        msg.meta["task"] = pod.metadata.labels["sagecontinuum.org/plugin-task"]
    except KeyError:
        raise InvalidMessageError(f"pod {pod.metadata.name} missing task label")


def convert_to_upload_message(msg: wagglemsg.Message, config: MessageHandlerConfig) -> wagglemsg.Message:
    return wagglemsg.Message(
        timestamp=msg.timestamp,
        name=config.upload_publish_name,
        meta=msg.meta,  # load_message is still the sole owner so no need to copy
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


def load_kube_config(kubeconfig: str):
    homeconfig = Path(Path.home(), ".kube/config").absolute()
    if kubeconfig is not None:
        kubernetes.config.load_kube_config(kubeconfig)
    elif homeconfig.exists():
        kubernetes.config.load_kube_config(str(homeconfig))
    else:
        kubernetes.config.load_incluster_config()


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
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(message)s",
        datefmt="%Y/%m/%d %H:%M:%S",
    )
    # turn down the super verbose library level logging
    logging.getLogger("kubernetes").setLevel(logging.CRITICAL)
    logging.getLogger("pika").setLevel(logging.CRITICAL)

    # NOTE the service account needs read access to pod info
    load_kube_config(args.kubeconfig)

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
    )

    logging.info(
        "connecting to rabbitmq server at %s:%d as %s.",
        params.host,
        params.port,
        params.credentials.username,
    )
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    logging.info("setting up queues and exchanges.")
    channel.exchange_declare("data.topic", exchange_type="topic", durable=True)
    declare_exchange_with_queue(channel, "to-validator")
    declare_exchange_with_queue(channel, "to-beehive")

    pod_event_watcher = PluginPodEventWatcher()

    # start metrics server
    start_http_server(args.metrics_port)

    handler = MessageHandler(
        config=MessageHandlerConfig(
            node=args.waggle_node_id,
            vsn=args.waggle_node_vsn,
            upload_publish_name=args.upload_publish_name,
        ),
        connection=connection,
        channel=channel,
        pod_event_watcher=pod_event_watcher,
    )

    logging.info("starting main process.")
    logging.info("will publish uploads under name %r", args.upload_publish_name)
    amqp.consume(channel, "to-validator", handler.handle)
    channel.start_consuming()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
