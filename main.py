import argparse
import json
import logging
import sys
from os import getenv
import kubernetes
import pika
import wagglemsg
from pathlib import Path


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

    try:
        pod = appstate.pods[properties.app_id]
    except KeyError as key:
        raise InvalidMessageError(f"no pod with uid {key}")

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


# NOTE update_pod_node_names introduces coupling between the data pipeline and
# kubernetes... however, conceptually we'll just coupling message data with scheduling
# metadata. for now, that scheduler is kubernetes, which is why we're talking directly
# to it. (kubernetes is the only thing which *actually* knows where a Pod is assigned)
def update_pod_node_names(pods: dict):
    """update_pod_node_names updates pods to the current {Pod UID: Pod Info} state."""
    logging.info("updating pod table...")
    pods.clear()
    v1api = kubernetes.client.CoreV1Api()
    ret = v1api.list_namespaced_pod("")
    for pod in ret.items:
        # only pods which have been scheduled and have nodeName metadata
        if not (isinstance(pod.spec.node_name, str) and pod.spec.node_name != ""):
            continue
        pods[pod.metadata.uid] = pod
        logging.info("adding pod %s", pod.metadata.name)
    logging.info("updated pod table")


def create_on_validator_callback(appstate):
    def on_validator_callback(ch, method, properties, body):
        logging.debug("processing message")

        # update cached pod info when we receive an unknown pod UID
        # TODO think about rogue case where a made up UID is rapidly sent
        if properties.app_id is not None and properties.app_id not in appstate.pods:
            logging.info("got new pod uid %s", properties.app_id)
            update_pod_node_names(appstate.pods)
            # TODO think about sending info message indicating we have data from this Pod now.
            # this could be sent up to further bind metadata together on the cloud.

        try:
            msg = load_message(appstate, properties, body)
            publish_message(ch, method.routing_key, msg)
        except InvalidMessageError as e:
            logging.error("invalid message: %s", e)
            ch.basic_ack(method.delivery_tag)
            return

        ch.basic_ack(method.delivery_tag)
        logging.debug("processed message")

    return on_validator_callback


def declare_exchange_with_queue(
    ch: pika.adapters.blocking_connection.BlockingChannel, name: str
):
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
    # pika logging is too verbose, so we turn it down.
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

    appstate = AppState(
        node=args.waggle_node_id,
        vsn=args.waggle_node_vsn,
        upload_publish_name=args.upload_publish_name,
    )

    logging.info("starting main process.")
    logging.info("will publish uploads under name %r.", appstate.upload_publish_name)
    channel.basic_consume("to-validator", create_on_validator_callback(appstate))
    channel.start_consuming()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
