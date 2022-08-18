import unittest
import json
import pika
import time
import wagglemsg

from contextlib import ExitStack
from prometheus_client.parser import text_string_to_metric_families
from redis import Redis
from uuid import uuid4
from urllib.request import urlopen
from waggle.plugin import Plugin, PluginConfig

TEST_RABBITMQ_HOST = "wes-rabbitmq"
TEST_RABBITMQ_PORT = 5672
TEST_RABBITMQ_USERNAME = "guest"
TEST_RABBITMQ_PASSWORD = "guest"

TEST_APP_META_CACHE_HOST = "wes-app-meta-cache"

TEST_DATA_SHARING_SERVICE_HOST = "wes-data-sharing-service"


def get_metrics():
    with urlopen(f"http://{TEST_DATA_SHARING_SERVICE_HOST}:8080") as f:
        text = f.read().decode()
    return {s.name: s.value for metric in text_string_to_metric_families(text) for s in metric.samples if s.name.startswith("wes_")}


def get_plugin(app_id):
    return Plugin(PluginConfig(
        host=TEST_RABBITMQ_HOST,
        port=TEST_RABBITMQ_PORT,
        username=TEST_RABBITMQ_USERNAME,
        password=TEST_RABBITMQ_PASSWORD,
        app_id=app_id,
    ))


class TestService(unittest.TestCase):

    def setUp(self):
        self.maxDiff = 4096

        self.exit_stack = ExitStack()

        self.redis = self.exit_stack.enter_context(Redis(TEST_APP_META_CACHE_HOST))
        self.redis.flushall()

        self.connection = self.exit_stack.enter_context(pika.BlockingConnection(pika.ConnectionParameters(
            host=TEST_RABBITMQ_HOST,
            port=TEST_RABBITMQ_PORT,
            credentials=pika.PlainCredentials(
                username=TEST_RABBITMQ_USERNAME,
                password=TEST_RABBITMQ_PASSWORD,
            )
        )))
        self.channel = self.exit_stack.enter_context(self.connection.channel())
        self.channel.queue_purge("to-validator")
        self.channel.queue_purge("to-beehive")

        # save current metrics for comparisons during tests
        self.metrics_at_setup = get_metrics()

        # save current timestamp for comparisons during tests
        self.timestamp = time.time_ns()
    
    def tearDown(self):
        self.exit_stack.close()
    
    def updateAppMetaCache(self, app_uid, meta):
        self.redis.set(f"app-meta.{app_uid}", json.dumps(meta))

    def assertMessages(self, queue, messages, timeout=1.0):
        results = []

        def on_message_callback(ch, method, properties, body):
            results.append(wagglemsg.load(body))
            if len(results) >= len(messages):
                ch.stop_consuming()

        self.connection.call_later(timeout, self.channel.stop_consuming)
        self.channel.basic_consume(queue, on_message_callback)
        self.channel.start_consuming()

        self.assertEqual(results, messages)

    def getSubscriber(self, topics):
        subscriber = self.exit_stack.enter_context(get_plugin(""))
        subscriber.subscribe(topics)
        time.sleep(0.1)
        return subscriber

    def assertSubscriberMessages(self, subscriber, messages):
        for msg in messages:
            self.assertEqual(msg, subscriber.get(timeout=1.0))

    def assertMetricsChanged(self, changes):
        metrics = get_metrics()
        diffs = {k: metrics[k] - self.metrics_at_setup[k] for k in metrics.keys()}
        for k, v in changes.items():
            self.assertAlmostEqual(diffs[k], v)

    def getPublishTestCases(self):
        # TODO(sean) should we fuzz test this to try lot's of different arguments
        messages = [
            wagglemsg.Message(
                name="test",
                value=1234,
                timestamp=time.time_ns(),
                meta={},
            ),
            wagglemsg.Message(
                name="e",
                value=2.71828,
                timestamp=time.time_ns(),
                meta={"user": "data"},
            ),
            wagglemsg.Message(
                name="replace.app.meta.with.sys.meta",
                value="should replace meta with app and sys meta",
                timestamp=time.time_ns(),
                meta={
                    "vsn": "Z123",
                    "job": "sure",
                    "task": "ok",
                },
            ),
        ]

        # NOTE(sean) this is defined in the wes-data-sharing-service's env vars in docker-compose.yaml
        sys_meta = {
            "node": "0000000000000001",
            "vsn": "W001",
        }

        app_uid = str(uuid4())
        app_meta = {
            "job": "sage",
            "task": "testing",
            "host": "1111222233334444.ws-nxcore",
            "vsn": "should be replaced",
        }
        self.updateAppMetaCache(app_uid, app_meta)

        # we expect the same messages, but with the app and sys meta tagged
        want_messages = [
            wagglemsg.Message(
                name=msg.name,
                value=msg.value,
                timestamp=msg.timestamp,
                # NOTE(sean) the order of meta is important. we should expect:
                # 1. sys meta overrides msg meta and app meta
                # 2. app meta overrides msg meta
                meta={**msg.meta, **app_meta, **sys_meta})
            for msg in messages
        ]

        return app_uid, messages, want_messages

    def publishMessages(self, app_uid, messages, scope):
        with get_plugin(app_uid) as plugin:
            for msg in messages:
                plugin.publish(msg.name, msg.value, timestamp=msg.timestamp, meta=msg.meta, scope=scope)

    def test_publish_beehive(self):
        app_uid, messages, want_messages = self.getPublishTestCases()
        self.publishMessages(app_uid, messages, scope="beehive")
        self.assertMessages("to-beehive", want_messages)
        self.assertMetricsChanged({
            "wes_data_service_messages_total": len(want_messages),
            "wes_data_service_messages_rejected_total": 0,
            "wes_data_service_messages_published_node_total": 0,
            "wes_data_service_messages_published_beehive_total": len(want_messages),
        })
    
    def test_publish_node(self):
        app_uid, messages, want_messages = self.getPublishTestCases()
        subscriber = self.getSubscriber("#")
        self.publishMessages(app_uid, messages, scope="node")
        self.assertSubscriberMessages(subscriber, want_messages)
        self.assertMetricsChanged({
            "wes_data_service_messages_total": len(want_messages),
            "wes_data_service_messages_rejected_total": 0,
            "wes_data_service_messages_published_node_total": len(want_messages),
            "wes_data_service_messages_published_beehive_total": 0,
        })

    def test_publish_all(self):
        app_uid, messages, want_messages = self.getPublishTestCases()
        subscriber = self.getSubscriber("#")
        self.publishMessages(app_uid, messages, scope="all")
        self.assertSubscriberMessages(subscriber, want_messages)
        self.assertMessages("to-beehive", want_messages)
        self.assertMetricsChanged({
            "wes_data_service_messages_total": len(want_messages),
            "wes_data_service_messages_rejected_total": 0,
            "wes_data_service_messages_published_node_total": len(want_messages),
            "wes_data_service_messages_published_beehive_total": len(want_messages),
        })

    def test_subscribe_topic(self):
        app_uid, messages, want_messages = self.getPublishTestCases()

        subscriber1 = self.getSubscriber("test")
        subscriber2 = self.getSubscriber("e")

        self.publishMessages(app_uid, messages, scope="all")

        self.assertSubscriberMessages(subscriber1, [msg for msg in want_messages if msg.name == "test"])
        self.assertSubscriberMessages(subscriber2, [msg for msg in want_messages if msg.name == "e"])

        self.assertMetricsChanged({
            "wes_data_service_messages_total": len(want_messages),
            "wes_data_service_messages_rejected_total": 0,
            "wes_data_service_messages_published_node_total": len(want_messages),
            "wes_data_service_messages_published_beehive_total": len(want_messages),
        })

    def test_bad_message_body(self):
        app_uid = str(uuid4())
        self.channel.basic_publish("to-validator", "all", b"{bad data", properties=pika.BasicProperties(app_id=app_uid))
        
        time.sleep(0.1)

        self.assertMetricsChanged({
            "wes_data_service_messages_total": 1,
            "wes_data_service_messages_rejected_total": 1,
            "wes_data_service_messages_published_node_total": 0,
            "wes_data_service_messages_published_beehive_total": 0,
        })

    def test_no_app_uid(self):
        with get_plugin("") as plugin:
            plugin.publish("test", 123)

        time.sleep(0.1)

        self.assertMetricsChanged({
            "wes_data_service_messages_total": 1,
            "wes_data_service_messages_rejected_total": 1,
            "wes_data_service_messages_published_node_total": 0,
            "wes_data_service_messages_published_beehive_total": 0,
        })

    def test_no_app_meta(self):
        app_uid = str(uuid4())

        with get_plugin(app_uid) as plugin:
            plugin.publish("test", 123)

        time.sleep(0.1)

        self.assertMetricsChanged({
            "wes_data_service_messages_total": 1,
            "wes_data_service_messages_rejected_total": 1,
            "wes_data_service_messages_published_node_total": 0,
            "wes_data_service_messages_published_beehive_total": 0,
        })


if __name__ == "__main__":
    unittest.main()
