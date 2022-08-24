import unittest
import json
import logging
import os
import pika
import time
import wagglemsg

from contextlib import ExitStack
from prometheus_client.parser import text_string_to_metric_families
from redis import Redis
from uuid import uuid4
from urllib.request import urlopen
from random import shuffle, randint
from pathlib import Path
from waggle.plugin import Plugin, PluginConfig

from tempfile import TemporaryDirectory
import threading

from main import Service, AppMetaCache

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "127.0.0.1")
RABBITMQ_PORT = int(os.environ.get("RABBITMQ_PORT", "5672"))

APP_META_CACHE_HOST = os.environ.get("APP_META_CACHE_HOST", "127.0.0.1")
APP_META_CACHE_PORT = int(os.environ.get("APP_META_CACHE_PORT", "6379"))

DATA_SHARING_SERVICE_HOST = os.environ.get("DATA_SHARING_SERVICE_HOST", "127.0.0.1")
DATA_SHARING_SERVICE_METRICS_PORT = int(os.environ.get("DATA_SHARING_SERVICE_METRICS_PORT", "8080"))


def get_plugin(app_id):
    return Plugin(PluginConfig(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        username="plugin",
        password="plugin",
        app_id=app_id,
    ))


def get_metrics():
    with urlopen(f"http://{DATA_SHARING_SERVICE_HOST}:{DATA_SHARING_SERVICE_METRICS_PORT}") as f:
        text = f.read().decode()
    return {s.name: s.value for metric in text_string_to_metric_families(text) for s in metric.samples if s.name.startswith("wes_")}


class TestService(unittest.TestCase):

    def setUp(self):
        self.es = ExitStack()

        self.service = Service(
            # rabbitmq config
            connection_parameters=pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                port=RABBITMQ_PORT,
                credentials=pika.PlainCredentials(
                    username="service",
                    password="service",
                ),
                client_properties={"name": "wes-data-sharing-service"},
                connection_attempts=3,
                retry_delay=10,
            ),
            src_queue="to-validator",
            dst_exchange_beehive="to-beehive",
            dst_exchange_node="data.topic",

            # metrics config
            metrics_host="0.0.0.0",
            metrics_port=8080,

            upload_publish_name="upload",

            # app meta cache config
            app_meta_cache=AppMetaCache(APP_META_CACHE_HOST),

            system_meta={
                "node": "0000000000000001",
                "vsn": "W001",
            },
            system_users=["service"],
        )

        # turn off info logging for unit tests
        self.service.logger.setLevel(logging.ERROR)

        self.clearAppMetaCache()

        # setup rabbitmq connection to purge queues for testing
        self.connection = self.es.enter_context(pika.BlockingConnection(pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            credentials=pika.PlainCredentials(
                username="admin",
                password="admin",
            )
        )))
        self.channel = self.es.enter_context(self.connection.channel())
        self.channel.queue_purge(self.service.src_queue)
        self.channel.queue_purge(self.service.dst_exchange_beehive)

        # setup upload dir
        # NOTE(sean) pywaggle uses /run/waggle as WAGGLE_PLUGIN_UPLOAD_PATH default. we hack this for now so we can run these unit tests.
        self.upload_dir = self.es.enter_context(TemporaryDirectory())
        os.environ["WAGGLE_PLUGIN_UPLOAD_PATH"] = str(Path(self.upload_dir).absolute())

        # run new background instance of service for testing
        threading.Thread(target=self.service.run, daemon=True).start()
        self.es.callback(self.service.shutdown)

    def tearDown(self):
        self.es.close()

    def clearAppMetaCache(self):
        with Redis(APP_META_CACHE_HOST) as redis:
            redis.flushall()

    def updateAppMetaCache(self, app_uid, meta):
        with Redis(APP_META_CACHE_HOST) as redis:
            redis.set(f"app-meta.{app_uid}", json.dumps(meta))

    def getSubscriber(self, topics):
        subscriber = self.es.enter_context(get_plugin(""))
        subscriber.subscribe(topics)
        time.sleep(0.1)
        return subscriber

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

    def assertSubscriberMessages(self, subscriber, messages):
        for msg in messages:
            self.assertEqual(msg, subscriber.get(timeout=1.0))

    def assertMetrics(self, want_metrics):
        metrics = get_metrics()
        for k, v in want_metrics.items():
            self.assertAlmostEqual(metrics[k], v)

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

        # randomize order of messages
        shuffle(messages)

        app_uid = str(uuid4())
        app_meta = {
            "job": f"sage-{randint(1, 1000000)}",
            "task": f"testing-{randint(1, 1000000)}",
            "host": f"{randint(1, 1000000)}.ws-nxcore",
            "plugin": f"plugin-test:{randtag()}",
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
                meta={**msg.meta, **app_meta, **self.service.system_meta})
            for msg in messages
        ]

        return app_uid, messages, want_messages

    def publishMessages(self, app_uid, messages, scope):
        with get_plugin(app_uid) as plugin:
            for msg in messages:
                plugin.publish(msg.name, msg.value, timestamp=msg.timestamp, meta=msg.meta, scope=scope)

    def testPublishBeehive(self):
        app_uid, messages, want_messages = self.getPublishTestCases()
        self.publishMessages(app_uid, messages, scope="beehive")
        self.assertMessages("to-beehive", want_messages)
        self.assertMetrics({
            "wes_data_service_messages_total": len(want_messages),
            "wes_data_service_messages_rejected_total": 0,
            "wes_data_service_messages_published_node_total": 0,
            "wes_data_service_messages_published_beehive_total": len(want_messages),
        })
    
    def testPublishNode(self):
        app_uid, messages, want_messages = self.getPublishTestCases()
        subscriber = self.getSubscriber("#")
        self.publishMessages(app_uid, messages, scope="node")
        self.assertSubscriberMessages(subscriber, want_messages)
        self.assertMetrics({
            "wes_data_service_messages_total": len(want_messages),
            "wes_data_service_messages_rejected_total": 0,
            "wes_data_service_messages_published_node_total": len(want_messages),
            "wes_data_service_messages_published_beehive_total": 0,
        })

    def testPublishAll(self):
        app_uid, messages, want_messages = self.getPublishTestCases()
        subscriber = self.getSubscriber("#")
        self.publishMessages(app_uid, messages, scope="all")
        self.assertSubscriberMessages(subscriber, want_messages)
        self.assertMessages("to-beehive", want_messages)
        self.assertMetrics({
            "wes_data_service_messages_total": len(want_messages),
            "wes_data_service_messages_rejected_total": 0,
            "wes_data_service_messages_published_node_total": len(want_messages),
            "wes_data_service_messages_published_beehive_total": len(want_messages),
        })

    def testSubscribeTopic(self):
        app_uid, messages, want_messages = self.getPublishTestCases()

        subscriber1 = self.getSubscriber("test")
        subscriber2 = self.getSubscriber("e")

        self.publishMessages(app_uid, messages, scope="all")

        self.assertSubscriberMessages(subscriber1, [msg for msg in want_messages if msg.name == "test"])
        self.assertSubscriberMessages(subscriber2, [msg for msg in want_messages if msg.name == "e"])

        self.assertMetrics({
            "wes_data_service_messages_total": len(want_messages),
            "wes_data_service_messages_rejected_total": 0,
            "wes_data_service_messages_published_node_total": len(want_messages),
            "wes_data_service_messages_published_beehive_total": len(want_messages),
        })

    def testBadMessageBody(self):
        app_uid = str(uuid4())
        self.channel.basic_publish("to-validator", "all", b"{bad data", properties=pika.BasicProperties(app_id=app_uid))
        
        time.sleep(0.1)

        self.assertMetrics({
            "wes_data_service_messages_total": 1,
            "wes_data_service_messages_rejected_total": 1,
            "wes_data_service_messages_published_node_total": 0,
            "wes_data_service_messages_published_beehive_total": 0,
        })

    def testNoAppUID(self):
        with get_plugin("") as plugin:
            plugin.publish("test", 123)

        time.sleep(0.1)

        self.assertMetrics({
            "wes_data_service_messages_total": 1,
            "wes_data_service_messages_rejected_total": 1,
            "wes_data_service_messages_published_node_total": 0,
            "wes_data_service_messages_published_beehive_total": 0,
        })

    def testNoAppMeta(self):
        app_uid = str(uuid4())

        with get_plugin(app_uid) as plugin:
            plugin.publish("test", 123)

        time.sleep(0.1)

        self.assertMetrics({
            "wes_data_service_messages_total": 1,
            "wes_data_service_messages_rejected_total": 1,
            "wes_data_service_messages_published_node_total": 0,
            "wes_data_service_messages_published_beehive_total": 0,
        })

    def testSystemServicePublish(self):
        messages = [
            wagglemsg.Message(
                name="system-message",
                value=123,
                meta={},
                timestamp=time.time_ns(),
            ),
            wagglemsg.Message(
                name="another-system-message",
                value=123,
                meta={},
                timestamp=time.time_ns(),
            ),
        ]

        want_messages = [
            wagglemsg.Message(
                name=msg.name,
                value=msg.value,
                timestamp=msg.timestamp,
                # notice that there is not app meta for system users
                meta={**msg.meta, **self.service.system_meta})
            for msg in messages
        ]

        with ExitStack() as es:
            # TODO(sean) try to consolidate this with existing testing scaffolding
            conn = es.enter_context(pika.BlockingConnection(pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                port=RABBITMQ_PORT,
                credentials=pika.PlainCredentials(
                    username="service",
                    password="service",
                )
            )))
            ch = es.enter_context(conn.channel())

            for msg in messages:
                properties = pika.BasicProperties(user_id="service")
                ch.basic_publish("to-validator", "all", wagglemsg.dump(msg), properties=properties)

        time.sleep(0.1)

        self.assertMessages("to-beehive", want_messages)

        self.assertMetrics({
            "wes_data_service_messages_total": len(want_messages),
            "wes_data_service_messages_rejected_total": 0,
            "wes_data_service_messages_published_node_total": len(want_messages),
            "wes_data_service_messages_published_beehive_total": len(want_messages),
        })

    def testPublishUpload(self):
        # TODO(sean) clean up! added as a regression test for now.
        app_uid = str(uuid4())

        tag = randtag()

        app_meta = {
            "job": f"sage-{randint(1, 1000000)}",
            "task": f"testing-{randint(1, 1000000)}",
            "host": f"{randint(1, 1000000)}.ws-nxcore",
            "plugin": f"plugin-test:{tag}",
            "vsn": "should be replaced",
        }
        self.updateAppMetaCache(app_uid, app_meta)

        timestamp = time.time_ns()
        filename = "hello.txt"

        with TemporaryDirectory() as dir:
            file = Path(dir, filename)
            file.write_text("hello")
            with get_plugin(app_uid) as plugin:
                plugin.upload_file(file, meta={"user": "data"}, timestamp=timestamp)

        job = app_meta["job"]
        task = app_meta["task"]
        node = self.service.system_meta["node"]

        self.assertMessages("to-beehive", [wagglemsg.Message(
            name="upload",
            value=f"https://storage.sagecontinuum.org/api/v1/data/{job}/sage-{task}-{tag}/{node}/{timestamp}-{filename}",
            timestamp=timestamp,
            meta={
                "user": "data",
                "filename": "hello.txt",
                **app_meta,
                **self.service.system_meta,
            }
        )])

        self.assertMetrics({
            "wes_data_service_messages_total": 1,
            "wes_data_service_messages_rejected_total": 0,
            "wes_data_service_messages_published_node_total": 1,
            "wes_data_service_messages_published_beehive_total": 1,
        })


def randtag():
    return f"{randint(0, 20)}.{randint(0, 20)}.{randint(0, 20)}"


if __name__ == "__main__":
    unittest.main()
