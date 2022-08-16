import argparse
import logging
import pika
import wagglemsg

from contextlib import ExitStack
from os import getenv
from prometheus_client import start_http_server, Counter
from redis import Redis


wes_data_service_messages_total = Counter("wes_data_service_messages_total", "Total number of messages handled.", [])
wes_data_service_messages_rejected_total = Counter("wes_data_service_messages_rejected_total", "Total number of invalid messages.")
wes_data_service_messages_published_node_total = Counter("wes_data_service_messages_published_node_total", "Total number of messages published to node.")
wes_data_service_messages_published_beehive_total = Counter("wes_data_service_messages_published_beehive_total", "Total number of messages published to beehive.")


class InvalidMessageError(Exception):
    def __init__(self, error):
        self.error = error


# class MessageHandler:
#     """
#     MessageHandler handles pod and message events and publishes messages triggered by these.

#     This implementation is designed to have the following behavior:

#     1. Messages with known pod metadata are immediately published.
#     2. Messages with unknown pod metadata are added to a backlog for the message's pod UID.
#     3. When a pod event is handled, the backlog for that Pod UID is immediately flushed.
#     4. Pod metadata expires after config.pod_state_expire_duration seconds. Any pod or
#        message events reset the expiration time for the message's pod UID. When a pod
#        expires all messages in the backlog are dropped.
#     """

#     logger = logging.getLogger("MessageHandler")

#     def __init__(self, config: MessageHandlerConfig, publisher, clock=MonotonicClock()):
#         self.config = config
#         self.pod_state = {}
#         self.publisher = publisher
#         self.clock = clock
#         self.deliveries_since_handle_expired_pods = 0

#     def handle_delivery(self, delivery: amqp.Delivery):
#         self.logger.debug("handling delivery...")
#         wes_data_service_messages_handled_total.inc()

#         if delivery.pod_uid is None:
#             self.logger.debug("dropping message without pod uid")
#             wes_data_service_messages_invalid_total.inc()
#             delivery.ack()
#             return

#         pod_state = self.get_or_create_pod_state(delivery.pod_uid)

#         if pod_state.pod is None:
#             self.logger.debug("adding delivery %s to backlog for %s", delivery, delivery.pod_uid)
#             wes_data_service_messages_backlogged_total.inc()
#             wes_data_service_messages_in_backlog.inc()
#             pod_state.backlog.append(delivery)
#         else:
#             self.load_and_publish_message(delivery)
#             pod_state.updated_at = self.clock.now()

#     def handle_pod(self, pod):
#         self.logger.debug("handling pod event %s (%s)...", pod.name, pod.uid)

#         pod_state = self.get_or_create_pod_state(pod.uid)
#         pod_state.pod = pod
#         pod_state.updated_at = self.clock.now()

#         self.flush_pod_backlog(pod.uid)

#     def get_or_create_pod_state(self, pod_uid):
#         if pod_uid in self.pod_state:
#             return self.pod_state[pod_uid]
#         pod_state = self.new_pod_state()
#         self.pod_state[pod_uid] = pod_state
#         wes_data_service_pods_in_backlog.inc()
#         return pod_state

#     def handle_expired_pods(self):
#         # TODO(sean) use pod status (ex. Running) to prolong life instead of blanket timeout
#         self.logger.debug("updating pod state...")

#         for pod_uid in list(self.pod_state.keys()):
#             pod_state = self.pod_state[pod_uid]

#             if not self.pod_state_expired(pod_state):
#                 continue

#             self.logger.debug("expiring pod state for %s", pod_uid)
#             for delivery in pod_state.backlog:
#                 delivery.ack()
#                 wes_data_service_messages_in_backlog.dec()
#                 wes_data_service_messages_expired_total.inc()
#             del self.pod_state[pod_uid]
#             wes_data_service_pods_in_backlog.dec()
#             wes_data_service_pods_expired_total.inc()

#         self.logger.debug("updated pod state")

#     def pod_state_expired(self, pod_state):
#         if pod_state.pod is None:
#             ttl = self.config.pod_without_metadata_state_expire_duration
#         else:
#             ttl = self.config.pod_state_expire_duration
#         return self.clock.now() - pod_state.updated_at > ttl

#     def flush_pod_backlog(self, pod_uid):
#         self.logger.debug("flushing pod backlog for %s...", pod_uid)
#         pod_state = self.pod_state[pod_uid]
#         if pod_state.pod is None:
#             return
#         for delivery in pod_state.backlog:
#             self.load_and_publish_message(delivery)
#             wes_data_service_messages_in_backlog.dec()
#         pod_state.backlog.clear()
#         self.logger.debug("flushed pod backlog")

#     def load_and_publish_message(self, delivery: amqp.Delivery):
#         self.logger.debug("publishing message %s...",  delivery)

#         try:
#             msg = wagglemsg.load(delivery.body)
#             update_message_with_config_metadata(msg, self.config)
#             update_message_with_pod_metadata(msg, self.pod_state[delivery.pod_uid].pod)
#         except Exception:
#             if self.logger.isEnabledFor(logging.DEBUG):
#                 self.logger.exception("failed to load waggle message")
#             delivery.ack()
#             wes_data_service_messages_invalid_total.inc()
#             return
        
#         if msg.name == "upload":
#             msg = convert_to_upload_message(msg, self.config)

#         body = wagglemsg.dump(msg)

#         if delivery.routing_key in ["node", "all"]:
#             self.logger.debug("forwarding message type %r to local", msg.name)
#             self.publisher.publish("data.topic", msg.name, amqp.Publishing(body))

#         if delivery.routing_key in ["beehive", "all"]:
#             self.logger.debug("forwarding message type %r to beehive", msg.name)
#             publishing = amqp.Publishing(body, pika.BasicProperties(delivery_mode=2))
#             self.publisher.publish("to-beehive", msg.name, publishing)

#         delivery.ack()
#         self.logger.debug("published message %s",  delivery)
#         wes_data_service_messages_published_total.inc()

#     def new_pod_state(self):
#         return PodState(pod=None, backlog=[], updated_at=self.clock.now())

#     def clear_backlogs_but_keep_pod_state(self):
#         for pod_state in self.pod_state.values():
#             pod_state.backlog.clear()
#         wes_data_service_messages_in_backlog.set(0)


# def update_message_with_config_metadata(msg: wagglemsg.Message, config: MessageHandlerConfig):
#     msg.meta["node"] = config.node
#     msg.meta["vsn"] = config.vsn


# def update_message_with_pod_metadata(msg: wagglemsg.Message, pod):
#     # add scheduler metadata using pod uid
#     msg.meta["host"] = pod.host
#     # TODO include namespace
#     msg.meta["plugin"] = pod.image.split("/")[-1]
#     msg.meta["job"] = pod.labels.get("sagecontinuum.org/plugin-job", "sage")

#     try:
#         msg.meta["task"] = pod.labels["sagecontinuum.org/plugin-task"]
#     except KeyError:
#         raise InvalidMessageError(f"pod {pod.name} missing task label")


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


class AppMetaCache:

    def __init__(self, host):
        self.client = Redis("wes-app-meta-cache")
    
    # add @lru_cache decorator
    def get(self, app_uid):
        key = f"app-meta.{app_uid}"
        return self.client.get(key)


class MessageHandler:

    logger = logging.getLogger("MessageHandler")

    def __init__(self, upload_publish_name="upload"):
        self.upload_publish_name = upload_publish_name
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

        # update metrics to total, err, ok (similar to http example prometheus gives)

        meta = self.app_meta_cache.get(app_uid)
        if meta is None:
            self.logger.info("reject msg: no pod meta: %r", msg)
            ch.basic_reject(method.delivery_tag, False)
            wes_data_service_messages_rejected_total.inc()
            return

        self.logger.info("meta: %s", meta)

        if msg.name == "upload":
            msg = convert_to_upload_message(msg, self.upload_publish_name)

        # update_message_with_config_metadata(msg, self.config)
        # update_message_with_pod_metadata(msg, self.pod_state[delivery.pod_uid].pod)
        msg = self.update_message_meta(msg)
        self.route_message(ch, method.routing_key, msg)
        ch.basic_ack(method.delivery_tag)
    
    def update_message_meta(self, msg: wagglemsg.Message) -> wagglemsg.Message:
        return msg
    
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

    message_handler = MessageHandler()

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
