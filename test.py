import unittest
from unittest.mock import MagicMock
from main import MessageHandler, MessageHandlerConfig, Publisher
from amqp import Delivery, Publishing
from pod_event_watcher import Pod
import wagglemsg


class TestMessageHandler(unittest.TestCase):

    def test_missing_uid(self):
        handler = MessageHandler(
            config=MessageHandlerConfig(
                node="0000000000000001",
                vsn="W001",
                upload_publish_name="upload",
            ),
            publisher=None,
        )
        delivery = Delivery(pod_uid=None)
        delivery.ack = MagicMock()
        handler.handle_delivery(delivery)

        delivery.ack.assert_called_once()
    
    def test_expect_task_label(self):
        publisher = Publisher(channel=None)
        publisher.publish = MagicMock()

        handler = MessageHandler(
            config=MessageHandlerConfig(
                node="0000000000000001",
                vsn="W001",
                upload_publish_name="upload",
            ),
            publisher=publisher,
        )

        pod = Pod(
            uid="some-uid",
            image="waggle/plugin-example:1.2.3",
            host="some-host",
            labels={},
        )

        delivery = Delivery(
            channel=None,
            delivery_tag=0,
            routing_key="all",
            pod_uid="some-uid",
            body=wagglemsg.dump(wagglemsg.Message(
                name="env.temperature",
                value=23.3,
                timestamp=123456.7,
                meta={},
            )),
        )
        delivery.ack = MagicMock()

        handler.handle_pod(pod)
        handler.handle_delivery(delivery)

        delivery.ack.assert_called_once()
        publisher.publish.assert_not_called()

    def test_publish_for_existing_pod(self):
        config = MessageHandlerConfig(
            node="0000000000000001",
            vsn="W001",
            upload_publish_name="upload",
        )

        publisher = Publisher(channel=None)
        publisher.publish = MagicMock()

        handler = MessageHandler(
            config=config,
            publisher=publisher,
        )

        pod = Pod(
            uid="some-uid",
            image="waggle/plugin-example:1.2.3",
            host="some-host",
            labels={
                "sagecontinuum.org/plugin-task": "example",
            },
        )

        msg = wagglemsg.Message(
            name="env.temperature",
            value=23.3,
            timestamp=123456.7,
            meta={},
        )

        body = wagglemsg.dump(msg)

        delivery = Delivery(
            channel=None,
            delivery_tag=0,
            routing_key="all",
            pod_uid="some-uid",
            body=body,
        )
        delivery.ack = MagicMock()

        handler.handle_pod(pod)
        handler.handle_delivery(delivery)

        delivery.ack.assert_called_once()

        self.assert_published(publisher, ["data.topic", "to-beehive"], wagglemsg.Message(
            name=msg.name,
            value=msg.value,
            timestamp=msg.timestamp,
            meta={
                "host": "some-host",
                "job": "sage",
                "task": "example",
                "plugin": "plugin-example:1.2.3",
                "node": config.node,
                "vsn": config.vsn,
            },
        ))

    def assert_published(self, publisher, exchanges, msg):
        calls = publisher.publish.call_args_list

        self.assertCountEqual(exchanges, [call.args[0] for call in calls])

        for call in calls:
            _, routing_key, publishing = call.args
            msgout = wagglemsg.load(publishing.body)
            self.assertEqual(routing_key, msg.name)
            self.assertEqual(msgout, msg)


if __name__ == "__main__":
    unittest.main()
