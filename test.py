import unittest
import json
import pika
import time
import requests
import wagglemsg

from contextlib import ExitStack
from prometheus_client.parser import text_string_to_metric_families
from redis import Redis
from uuid import uuid4
from waggle.plugin import Plugin, PluginConfig


def get_metrics():
    r = requests.get("http://127.0.0.1:8080")
    r.raise_for_status()
    return {s.name: s.value for metric in text_string_to_metric_families(r.text) for s in metric.samples if s.name.startswith("wes_")}


def assert_metrics_deltas(tc: unittest.TestCase, before, after, want_deltas):
    deltas = {k: after[k] - before[k] for k in before.keys()}
    for k, v in want_deltas.items():
        tc.assertAlmostEqual(deltas[k], v)


def get_plugin(app_id):
    return Plugin(PluginConfig(
        host="127.0.0.1",
        port=5672,
        username="guest",
        password="guest",
        app_id=app_id,
    ))


class TestService(unittest.TestCase):

    def setUp(self):
        self.maxDiff = 4096

        self.exit_stack = ExitStack()

        self.redis = self.exit_stack.enter_context(Redis())
        self.redis.flushall()

        self.connection = self.exit_stack.enter_context(pika.BlockingConnection(pika.ConnectionParameters()))
        self.channel = self.exit_stack.enter_context(self.connection.channel())
        self.channel.queue_purge("to-validator")
        self.channel.queue_purge("to-beehive")

        # save current metrics for comparisons during tests
        self.metrics_at_setup = get_metrics()

        # save current timestamp for comparisons during tests
        self.timestamp = time.time_ns()
    
    def tearDown(self):
        self.exit_stack.close()
    
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

    def assertMetricsChanged(self, changes):
        metrics = get_metrics()
        diffs = {k: metrics[k] - self.metrics_at_setup[k] for k in metrics.keys()}
        for k, v in changes.items():
            self.assertAlmostEqual(diffs[k], v)

    def test_publish_to_beehive(self):
        app_uid = str(uuid4())

        # add pod metadata
        self.redis.set(f"app-meta.{app_uid}", json.dumps({
            "job": "sage",
            "task": "testing",
            "host": "1111222233334444.ws-nxcore",
        }))

        # publish test message
        with get_plugin(app_uid) as plugin:
            plugin.publish(
                name="test",
                value=1234,
                timestamp=self.timestamp,
                scope="beehive",
            )
            plugin.publish(
                name="e",
                value=2.71828,
                timestamp=self.timestamp,
                meta={"user": "data"},
                scope="beehive",
            )

        self.assertMessages("to-beehive", [
            wagglemsg.Message(
                name="test",
                value=1234,
                timestamp=self.timestamp,
                meta={
                    "job": "sage",
                    "task": "testing",
                    "host": "1111222233334444.ws-nxcore",
                    "node": "0000000000000001",
                    "vsn": "W001",
                },
            ),
            wagglemsg.Message(
                name="e",
                value=2.71828,
                timestamp=self.timestamp,
                meta={
                    "job": "sage",
                    "task": "testing",
                    "host": "1111222233334444.ws-nxcore",
                    "node": "0000000000000001",
                    "vsn": "W001",
                    "user": "data",
                },
            ),
        ])

        self.assertMetricsChanged({
            "wes_data_service_messages_total": 2,
            "wes_data_service_messages_rejected_total": 0,
            "wes_data_service_messages_published_node_total": 0,
            "wes_data_service_messages_published_beehive_total": 2,
        })
    
    def test_publish_to_node(self):
        app_uid = str(uuid4())

        # add pod metadata
        self.redis.set(f"app-meta.{app_uid}", json.dumps({
            "job": "sage",
            "task": "testing",
            "host": "1111222233334444.ws-nxcore",
        }))

        # publish test message and make sure a subscriber receives it
        with get_plugin("") as consumer, get_plugin(app_uid) as publisher:
            consumer.subscribe("#")

            time.sleep(0.1)

            publisher.publish(
                name="test",
                value=1234,
                timestamp=self.timestamp,
                meta={"user": "data"},
                scope="node",
            )

            msg = consumer.get(timeout=1.0)

            self.assertEqual(msg, wagglemsg.Message(
                name="test",
                value=1234,
                timestamp=self.timestamp,
                meta={
                    "job": "sage",
                    "task": "testing",
                    "host": "1111222233334444.ws-nxcore",
                    "node": "0000000000000001",
                    "vsn": "W001",
                    "user": "data",
                },
            ))

        self.assertMetricsChanged({
            "wes_data_service_messages_total": 1,
            "wes_data_service_messages_rejected_total": 0,
            "wes_data_service_messages_published_node_total": 1,
            "wes_data_service_messages_published_beehive_total": 0,
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
