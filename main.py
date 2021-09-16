import argparse
import time
import random
import pika
import json
import os
import logging
import waggle.message as message
import re
import kubernetes


WAGGLE_NODE_ID = os.environ.get("WAGGLE_NODE_ID", "")
WAGGLE_NODE_VSN = os.environ.get("WAGGLE_NODE_VSN", "")
RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq-server")
RABBITMQ_PORT = int(os.environ.get("RABBITMQ_PORT", "5672"))
RABBITMQ_USERNAME = os.environ.get("RABBITMQ_USERNAME", "service")
RABBITMQ_PASSWORD = os.environ.get("RABBITMQ_PASSWORD", "service")


user_id_pattern_v1 = re.compile(r"^plugin\.(\S+:\d+\.\d+\.\d+)$")
user_id_pattern_v2 = re.compile(r"^plugin\.(\S+)-(\d+-\d+-\d+)-([0-9a-f]{8})$")


def match_plugin_user_id(s):
    # type sanity check
    if not isinstance(s, str):
        return None
    # match early user_id with no config / instance hash
    match = user_id_pattern_v1.match(s)
    if match is not None:
        return match.group(1)
    # match newer user_id which include config / instance hash
    # TODO(sean) look at how to incorporate the hash / instance info into the data stream,
    # if needed. using this directly in a meta field will blow up the data quite a lot and
    # have no semantic meaning. *maybe* the user will add more meaningful meta tags like
    # sensor or camera which will distinguish this enough.
    match = user_id_pattern_v2.match(s)
    if match is not None:
        return match.group(1) + ":" + match.group(2).replace("-", ".")
    return None


class InvalidMessageError(Exception):

    def __init__(self, error):
        self.error = error


def load_message(pods, properties, body):
    try:
        msg = message.load(body)
    except json.JSONDecodeError:
        raise InvalidMessageError(f"failed to parse message body {body}")
    except KeyError as key:
        raise InvalidMessageError(f"message missing key {key}")

    # add metadata derived from pod info
    if properties.app_id is not None:
        pod_uid = properties.app_id
        try:
            pod = pods[pod_uid]
        except KeyError:
            raise InvalidMessageError(f"unable to find pod node name for {pod_uid}")
        msg.meta["host"] = pod.spec.node_name
        # TODO use Pod spec to get actual image used
        # msg.meta["plugin"] = pod.spec.containers[0].image

    # add plugin metadata
    plugin = match_plugin_user_id(properties.user_id)
    if plugin is None:
        raise InvalidMessageError(f"invalid message user_id {properties.user_id!r}")
    msg.meta["plugin"] = plugin

    # add node metadata
    if WAGGLE_NODE_ID != "":
        msg.meta["node"] = WAGGLE_NODE_ID
    if WAGGLE_NODE_VSN != "":
        msg.meta["vsn"] = WAGGLE_NODE_VSN

    return msg


def publish_message(ch, scope, msg):
    body = message.dump(msg)

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


def update_pod_node_names(pods):
    # clear all existing items since we will read in everything that's actually Running
    pods.clear()
    # scan pods from kubernetes
    v1api = kubernetes.client.CoreV1Api()
    ret = v1api.list_namespaced_pod("default")
    for item in ret.items:
        pods[item.metadata.uid] = item


def create_on_validator_callback():
    pods = {}

    def on_validator_callback(ch, method, properties, body):
        logging.debug("processing message.")

        # update cached pod info when we receive an unknown pod UID
        # TODO think about rogue case where a made up UID is rapidly sent
        if properties.app_id is not None and properties.app_id not in pods:
            logging.info("got new pod uid %s. updating pod metadata...", properties.app_id)
            update_pod_node_names(pods)
            logging.info("updated pod metadata.")
            # TODO think about sending info message indicating we have data from this Pod now.
            # this could be sent up to further bind metadata together on the cloud.

        try:
            msg = load_message(pods, properties, body)
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
    parser.add_argument("--rabbitmq-host", default=RABBITMQ_HOST, help="rabbitmq host")
    parser.add_argument("--rabbitmq-port", default=RABBITMQ_PORT, type=int, help="rabbitmq port")
    parser.add_argument("--rabbitmq-username", default=RABBITMQ_USERNAME, help="rabbitmq username")
    parser.add_argument("--rabbitmq-password", default=RABBITMQ_PASSWORD, help="rabbitmq password")
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
    channel.basic_consume("to-validator", create_on_validator_callback())
    channel.start_consuming()

if __name__ == "__main__":
    main()
