import argparse
import json
import logging
import re
from os import getenv
import kubernetes
import pika
import wagglemsg


class AppState:
    """AppState encapsulates the config and state of this app. It's primarily used to store cached Pod info."""

    def __init__(self, node, vsn, pods={}):
        self.node = node
        self.vsn = vsn
        self.pods = pods


class InvalidMessageError(Exception):
    """InvalidMessageError is throw to indicate that a message is invalid. (Bad JSON data, missing fields, etc.)"""

    def __init__(self, error):
        self.error = error


user_id_prefix = re.compile("^plugin\.(.*)$")
user_id_pattern_v1 = re.compile(r"^((\S+):\d+\.\d+\.\d+)$")
user_id_pattern_v2 = re.compile(r"^(\S+)-(\d+-\d+-\d+)-([0-9a-f]{8})$")


def match_plugin_user_id(s: str):
    if not isinstance(s, str):
        return None, None
    
    # match and trim leading plugin. prefix
    match = user_id_prefix.match(s)
    if match is None:
        return None, None
    s = match.group(1)

    # match early user_id with no config / instance hash (plugin.plugin-metsense:1.2.3)
    match = user_id_pattern_v1.match(s)
    if match is not None:
        return match.group(2), match.group(1)

    # match newer user_id which include config / instance hash (plugin.plugin-raingauge-1-2-3-ae43fc12)
    match = user_id_pattern_v2.match(s)
    if match is not None:
        return match.group(1), match.group(1) + ":" + match.group(2).replace("-", ".")

    # if doesn't match any of the previous patterns return name as-is
    return s, None


def upload_url_for_message(msg: wagglemsg.Message) -> str:
    try:
        job = msg.meta["job"]
        task = msg.meta["task"]
        node = msg.meta["node"]
        filename = msg.meta["filename"]
    except KeyError as exc:
        raise InvalidMessageError(f"message missing fields for upload url: {exc}")
    return f"https://storage.sagecontinuum.org/api/v1/data/{job}/{task}/{node}/{msg.timestamp}-{filename}"


def load_message(appstate: AppState, properties: pika.BasicProperties, body: bytes):
    try:
        msg = wagglemsg.load(body)
    except json.JSONDecodeError:
        raise InvalidMessageError(f"failed to parse message body {body}")
    except KeyError as key:
        raise InvalidMessageError(f"message missing key {key}")

    # add scheduler metadata using user_id (deprecated, will remove)
    task, plugin = match_plugin_user_id(properties.user_id)
    if task is None:
        raise InvalidMessageError(f"could not derive task name: user_id={properties.user_id}")
    if plugin is not None:
        msg.meta["plugin"] = plugin

    # add scheduler metadata using pod uid
    labels = {}

    if properties.app_id is not None:
        pod_uid = properties.app_id
        try:
            pod = appstate.pods[pod_uid]
        except KeyError:
            raise InvalidMessageError(f"unable to find pod node name for {pod_uid}")
        msg.meta["host"] = pod.spec.node_name
        # TODO include namespace
        msg.meta["plugin"] = pod.spec.containers[0].image.split("/")[-1]
        labels = pod.metadata.labels

    msg.meta["job"] = labels.get("sagecontinuum.org/plugin-job", "sage")
    msg.meta["task"] = labels.get("sagecontinuum.org/plugin-task", task)

    # add node metadata
    msg.meta["node"] = appstate.node
    msg.meta["vsn"] = appstate.vsn

    # rewrite upload messages to use url
    if msg.name == "upload":
        msg = wagglemsg.Message(
            timestamp=msg.timestamp,
            name=msg.name,
            meta=msg.meta, # load_message is still the sole owner so no need to copy
            value=upload_url_for_message(msg),
        )

    if "job" not in msg.meta:
        raise InvalidMessageError(f"could not infer message job: {msg!r}")
    if "task" not in msg.meta:
        raise InvalidMessageError(f"could not infer message task: {msg!r}")
    if "plugin" not in msg.meta:
        raise InvalidMessageError(f"could not infer message plugin: {msg!r}")

    return msg


def publish_message(ch, scope, msg):
    body = wagglemsg.dump(msg)

    if scope not in {"node", "beehive", "all"}:
        raise InvalidMessageError(f"invalid message scope {scope!r}")

    if scope in {"node", "all"}:
        logging.debug("forwarding message type %r to local", msg.name)
        ch.basic_publish(
            exchange="data.topic",
            routing_key=msg.name,
            body=body)

    if scope in {"beehive", "all"}:
        logging.debug("forwarding message type %r to beehive", msg.name)
        ch.basic_publish(
            exchange="to-beehive",
            routing_key=msg.name,
            body=body)


# NOTE update_pod_node_names introduces coupling between the data pipeline and
# kubernetes... however, conceptually we'll just coupling message data with scheduling
# metadata. for now, that scheduler is kubernetes, which is why we're talking directly
# to it. (kubernetes is the only thing which *actually* knows where a Pod is assigned)
def update_pod_node_names(pods: dict):
    """update_pod_node_names updates pods to the current {Pod UID: Pod Info} state."""
    pods.clear()
    v1api = kubernetes.client.CoreV1Api()
    ret = v1api.list_namespaced_pod("default")
    for pod in ret.items:
        pods[pod.metadata.uid] = pod


def create_on_validator_callback(appstate):
    def on_validator_callback(ch, method, properties, body):
        logging.debug("processing message.")

        # update cached pod info when we receive an unknown pod UID
        # TODO think about rogue case where a made up UID is rapidly sent
        if properties.app_id is not None and properties.app_id not in appstate.pods:
            logging.info("got new pod uid %s. updating pod metadata...", properties.app_id)
            update_pod_node_names(appstate.pods)
            logging.info("updated pod metadata.")
            # TODO think about sending info message indicating we have data from this Pod now.
            # this could be sent up to further bind metadata together on the cloud.

        try:
            msg = load_message(appstate, properties, body)
            publish_message(ch, method.routing_key, msg)
        except InvalidMessageError:
            # NOTE my assumption is that we generally should not have many invalid messages by
            # the time we've deployed code to the edge, so we'd like to know as much as possible
            # about them. we can dial this logging down if that turns out to be a bad assumption.
            logging.exception("dropping invalid message.")
            ch.basic_ack(method.delivery_tag)
            return

        ch.basic_ack(method.delivery_tag)
        logging.debug("processed message.")

    return on_validator_callback


def declare_exchange_with_queue(ch: pika.adapters.blocking_connection.BlockingChannel, name: str):
    ch.exchange_declare(name, exchange_type='fanout', durable=True)
    ch.queue_declare(name, durable=True)
    ch.queue_bind(name, name)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true", help="enable verbose logging")
    parser.add_argument("--kubeconfig", default=None, help="kubernetes config")
    parser.add_argument("--rabbitmq-host", default=getenv("RABBITMQ_HOST", "rabbitmq-server"), help="rabbitmq host")
    parser.add_argument("--rabbitmq-port", default=int(getenv("RABBITMQ_PORT", "5672")), type=int, help="rabbitmq port")
    parser.add_argument("--rabbitmq-username", default=getenv("RABBITMQ_USERNAME", "service"), help="rabbitmq username")
    parser.add_argument("--rabbitmq-password", default=getenv("RABBITMQ_PASSWORD", "service"), help="rabbitmq password")
    parser.add_argument("--waggle-node-id", default=getenv("WAGGLE_NODE_ID", "0000000000000000"), help="waggle node id")
    parser.add_argument("--waggle-node-vsn", default=getenv("WAGGLE_NODE_VSN", "W000"), help="waggle node vsn")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(message)s",
        datefmt="%Y/%m/%d %H:%M:%S")
    # pika logging is too verbose, so we turn it down.
    logging.getLogger("pika").setLevel(logging.CRITICAL)

    # load incluster service account config
    # NOTE the service account needs read access to pod info
    if args.kubeconfig is None:
        kubernetes.config.load_incluster_config()
    else:
        kubernetes.config.load_kube_config(args.kubeconfig)

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

    logging.info("connecting to rabbitmq server at %s:%d as %s.", params.host, params.port, params.credentials.username)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    logging.info("setting up queues and exchanges.")
    channel.exchange_declare("data.topic", exchange_type="topic", durable=True)
    declare_exchange_with_queue(channel, "to-validator")
    declare_exchange_with_queue(channel, "to-beehive")
    
    logging.info("starting main process.")
    appstate = AppState(node=args.waggle_node_id, vsn=args.waggle_node_vsn)
    channel.basic_consume("to-validator", create_on_validator_callback(appstate))
    channel.start_consuming()

if __name__ == "__main__":
    main()
