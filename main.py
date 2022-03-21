import argparse
from email import message
import json
import logging
import sys
from os import getenv
import kubernetes
import pika
import wagglemsg
from pathlib import Path
import time
from collections import defaultdict
from queue import Queue, Empty
from threading import Thread


class AppState:
    """AppState encapsulates the config and state of this app. It's primarily used to store cached Pod info."""

    def __init__(self, node, vsn, upload_publish_name, pods={}):
        self.node = node
        self.vsn = vsn
        self.upload_publish_name = upload_publish_name
        self.pods = pods


class InvalidMessageError(Exception):
    """InvalidMessageError is throw to indicate that a message is invalid. (Bad JSON data, missing fields, etc.)"""

    def __init__(self, error):
        self.error = error


def upload_url_for_message(msg: wagglemsg.Message) -> str:
    try:
        job = msg.meta["job"]
        task = msg.meta["task"]
        node = msg.meta["node"]
        filename = msg.meta["filename"]
        plugin = msg.meta["plugin"]
        namespace = "sage"
        version = plugin.split(":")[-1]
    except KeyError as exc:
        raise InvalidMessageError(f"message missing fields for upload url: {exc}")
    return f"https://storage.sagecontinuum.org/api/v1/data/{job}/{namespace}-{task}-{version}/{node}/{msg.timestamp}-{filename}"


def load_message(appstate: AppState, properties: pika.BasicProperties, body: bytes):
    try:
        msg = wagglemsg.load(body)
    except Exception:
        raise InvalidMessageError(f"failed to parse message body {body!r}")

    if properties.app_id is None:
        raise InvalidMessageError("message missing pod uid")

    pod = appstate.pods[properties.app_id]

    # add scheduler metadata using pod uid
    msg.meta["host"] = pod.spec.node_name
    # TODO include namespace
    msg.meta["plugin"] = pod.spec.containers[0].image.split("/")[-1]
    msg.meta["job"] = pod.metadata.labels.get("sagecontinuum.org/plugin-job", "sage")

    try:
        msg.meta["task"] = pod.metadata.labels["sagecontinuum.org/plugin-task"]
    except KeyError:
        raise InvalidMessageError(f"pod {pod.metadata.name} missing task label")

    # add node metadata
    msg.meta["node"] = appstate.node
    msg.meta["vsn"] = appstate.vsn

    # rewrite upload messages to use url
    if msg.name == "upload":
        msg = wagglemsg.Message(
            timestamp=msg.timestamp,
            name=appstate.upload_publish_name,
            meta=msg.meta,  # load_message is still the sole owner so no need to copy
            value=upload_url_for_message(msg),
        )

    return msg


def publish_message(ch, scope, msg):
    body = wagglemsg.dump(msg)

    if scope not in {"node", "beehive", "all"}:
        raise InvalidMessageError(f"invalid message scope {scope!r}")

    if scope in {"node", "all"}:
        logging.debug("forwarding message type %r to local", msg.name)
        ch.basic_publish(exchange="data.topic", routing_key=msg.name, body=body)

    if scope in {"beehive", "all"}:
        logging.debug("forwarding message type %r to beehive", msg.name)
        # messages to beehive are always marked as persistent
        properties = pika.BasicProperties(delivery_mode=2)
        ch.basic_publish(
            exchange="to-beehive",
            routing_key=msg.name,
            properties=properties,
            body=body,
        )


class PluginPodEventWatcher:

    def __init__(self):
        self.watch = kubernetes.watch.Watch()
        self.events = Queue()
        Thread(target=self.main, daemon=True).start()

    def stop(self):
        self.watch.stop()

    def main(self):
        v1 = kubernetes.client.CoreV1Api()
        for event in self.watch.stream(v1.list_pod_for_all_namespaces, label_selector="sagecontinuum.org/plugin-task"):
            pod = event["object"]
            if pod.spec.node_name is None:
                continue
            self.events.put(pod)


class MessageHandler:

    def __init__(self, connection: pika.BlockingConnection, channel, appstate: AppState):
        self.connection = connection
        self.channel = channel
        self.appstate = appstate
        self.backlog = defaultdict(list)
        self.prune_interval = 30.0
        self.update_pod_events_interval = 1.0
        self.pod_event_watcher = PluginPodEventWatcher()
        self.connection.call_later(self.prune_interval, self.prune_backlog_items)
        self.connection.call_later(self.update_pod_events_interval, self.update_pod_events)

    def handle(self, ch, method, properties, body):
        pod_uid = properties.app_id

        if pod_uid is None:
            logging.debug("dropping message without pod uid")
            ch.basic_ack(method.delivery_tag)
            return

        # messages without a known pod uid will be added to a backlog to allow us
        # to attempt to sync with pods from kubernetes
        if pod_uid not in self.appstate.pods:
            logging.debug("adding message %d to backlog", method.delivery_tag)
            self.backlog[pod_uid].append((method, properties, body, time.monotonic()))
            return

        self.load_and_publish_message(method, properties, body)
    
    def update_pod_events(self):
        self.connection.call_later(self.update_pod_events_interval, self.update_pod_events)

        while True:
            try:
                pod = self.pod_event_watcher.events.get_nowait()
            except Empty:
                break
            logging.info("adding pod %s (%s)", pod.metadata.name, pod.metadata.uid)
            self.appstate.pods[pod.metadata.uid] = pod
            self.flush_backlog_items_for_pod(pod.metadata.uid)
        # TODO(sean) cleanup pods

    def load_and_publish_message(self, method, properties, body):
        try:
            msg = load_message(self.appstate, properties, body)
            publish_message(self.channel, method.routing_key, msg)
        except InvalidMessageError as e:
            logging.error("invalid message: %s", e)
            self.channel.basic_ack(method.delivery_tag)
            return

        self.channel.basic_ack(method.delivery_tag)
        logging.debug("processed message %d",  method.delivery_tag)

    def flush_backlog_items_for_pod(self, pod_uid):
        try:
            messages = self.backlog[pod_uid]
        except KeyError:
            return
        for method, properties, body, _ in messages:
            self.load_and_publish_message(method, properties, body)
        pod = self.appstate.pods[pod_uid]
        logging.info("flushed %d messages from backlog for %s (%s)", len(messages), pod.metadata.name, pod_uid)
        del self.backlog[pod_uid]

    def prune_backlog_items(self):
        self.connection.call_later(self.prune_interval, self.prune_backlog_items)

        if len(self.backlog) == 0:
            logging.info("backlog is empty - skipping prune.")
            return

        logging.info("pruning backlog")

        # NOTE we run this infrequently (~10s), so we will just rebuild the entire data structure each time.
        now = time.monotonic()

        pruned_backlog = defaultdict(list)

        for pod_uid, messages in self.backlog.items():
            for method, properties, body, recv_time in messages:
                # keep fresh items
                if now - recv_time < 30.0:
                    pruned_backlog[pod_uid].append((method, properties, body, recv_time))
                    continue
                # expire stale items
                logging.debug("expire message %d", method.delivery_tag)
                self.channel.basic_ack(method.delivery_tag)

        log_backlog_size_change(backlog_size(self.backlog), backlog_size(pruned_backlog))
        self.backlog = pruned_backlog


def backlog_size(backlog):
    return sum(map(len, backlog.values()))


def log_backlog_size_change(old_size, new_size):
    logging.info("pruned %d of %d items from backlog", old_size - new_size, old_size)


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
    parser.add_argument("--kubeconfig", default=None, help="kubernetes config")
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

    handler = MessageHandler(
        connection=connection,
        channel=channel,
        appstate=AppState(
            node=args.waggle_node_id,
            vsn=args.waggle_node_vsn,
            upload_publish_name=args.upload_publish_name,
        ),
    )

    logging.info("starting main process.")
    logging.info("will publish uploads under name %r.", args.upload_publish_name)
    channel.basic_consume("to-validator", handler.handle)
    channel.start_consuming()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
