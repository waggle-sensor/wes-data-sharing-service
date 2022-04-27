import unittest
from unittest.mock import MagicMock
from main import MessageHandler, MessageHandlerConfig, Publisher
from amqp import Delivery, Publishing
from pod_event_watcher import Pod
import wagglemsg


class MockClock:

    def __init__(self, time):
        self.time = time
    
    def now(self):
        return self.time


def make_test_handler():
    return MessageHandler(
        config=MessageHandlerConfig(
            node="0000000000000001",
            vsn="W001",
            upload_publish_name="upload",
        ),
        publisher=Publisher(channel=None),
        clock=MockClock(0),
    )


class TestMessageHandler(unittest.TestCase):

    def test_missing_uid(self):
        handler = make_test_handler()
        delivery = Delivery(pod_uid=None)

        handler.publisher.publish = MagicMock()
        delivery.ack = MagicMock()
        
        # 1. get bad delivery
        handler.handle_delivery(delivery)

        # 2. message should not be published and should be dropped
        handler.publisher.publish.assert_not_called()
        delivery.ack.assert_called_once()
    
    def test_expect_task_label(self):
        handler = make_test_handler()

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
            pod_uid=pod.uid,
            body=wagglemsg.dump(wagglemsg.Message(
                name="env.temperature",
                value=23.3,
                timestamp=123456.7,
                meta={},
            )),
        )

        handler.publisher.publish = MagicMock()
        delivery.ack = MagicMock()

        # 1. get pod without task
        handler.handle_pod(pod)

        # 2. get delivery
        handler.handle_delivery(delivery)

        # 3. message should not be called and should be dropped
        handler.publisher.publish.assert_not_called()
        delivery.ack.assert_called_once()

    def test_handle_pod_then_delivery(self):
        handler = make_test_handler()

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
            pod_uid=pod.uid,
            body=body,
        )

        handler.publisher.publish = MagicMock()
        delivery.ack = MagicMock()

        # 1. get pod
        handler.handle_pod(pod)

        # 2. get delivery
        handler.handle_delivery(delivery)

        # 3. message should be published to locally and to beehive and acked
        self.assert_published(handler.publisher, ["data.topic", "to-beehive"], wagglemsg.Message(
            name=msg.name,
            value=msg.value,
            timestamp=msg.timestamp,
            meta={
                "host": "some-host",
                "job": "sage",
                "task": "example",
                "plugin": "plugin-example:1.2.3",
                "node": handler.config.node,
                "vsn": handler.config.vsn,
            },
        ))
        delivery.ack.assert_called_once()

    def test_handle_delivery_then_pod(self):
        handler = make_test_handler()

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
            pod_uid=pod.uid,
            body=body,
        )

        handler.publisher.publish = MagicMock()
        delivery.ack = MagicMock()

        # 1. get delivery
        handler.handle_delivery(delivery)

        # 2. nothing should have happened yet since we don't have a pod
        delivery.ack.assert_not_called()
        handler.publisher.publish.assert_not_called()

        # 3. delivery should still be unacked before "non metadata" expire time
        handler.clock.time += handler.config.pod_without_metadata_state_expire_duration
        handler.handle_expired_pods()
        delivery.ack.assert_not_called()

        # 4. get pod
        handler.handle_pod(pod)

        # 5. message should be published to locally and to beehive and acked
        self.assert_published(handler.publisher, ["data.topic", "to-beehive"], wagglemsg.Message(
            name=msg.name,
            value=msg.value,
            timestamp=msg.timestamp,
            meta={
                "host": "some-host",
                "job": "sage",
                "task": "example",
                "plugin": "plugin-example:1.2.3",
                "node": handler.config.node,
                "vsn": handler.config.vsn,
            },
        ))
        delivery.ack.assert_called_once()
    
    def test_backlog_should_expire_without_metadata(self):
        handler = make_test_handler()
        handler.publisher.publish = MagicMock()

        # generate a bunch of deliveries
        deliveries = []

        for i in range(23):
            delivery = Delivery(
                channel=None,
                delivery_tag=i,
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
            deliveries.append(delivery)

        # 1. get all deliveries
        for delivery in deliveries:
            handler.handle_delivery(delivery)

        # 2. nothing should have happened yet since we don't have a pod
        handler.publisher.publish.assert_not_called()
        for delivery in deliveries:
            delivery.ack.assert_not_called()

        # 3. deliveries should still be valid up until "non metadata" expire time
        handler.clock.time += handler.config.pod_without_metadata_state_expire_duration
        handler.handle_expired_pods()
        for delivery in deliveries:
            delivery.ack.assert_not_called()

        # 4. deliveries should expire after one more tick
        handler.clock.time += 1
        handler.handle_expired_pods()
        for delivery in deliveries:
            delivery.ack.assert_called_once()

        # 5. deliveries should be removed as to not "double expire"
        handler.handle_expired_pods()
        for delivery in deliveries:
            delivery.ack.assert_called_once()

        # 6. nothing should have been published
        handler.publisher.publish.assert_not_called()

    def test_stale_pod_should_expire(self):
        handler = make_test_handler()

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
            pod_uid=pod.uid,
            body=body,
        )

        handler.publisher.publish = MagicMock()
        delivery.ack = MagicMock()

        # 1. get pod
        handler.handle_pod(pod)

        # 2. pod should expire
        handler.clock.time += handler.config.pod_state_expire_duration + 1
        handler.handle_expired_pods()

        # 3. now that pod is expired, new delivery shouldn't be flushed right away
        handler.handle_delivery(delivery)
        delivery.ack.assert_not_called()

        # 4. after this, delivery should expire after "without metadata" duration
        handler.clock.time += handler.config.pod_without_metadata_state_expire_duration + 1
        handler.handle_expired_pods()

        # 5. message should not be published and should be dropped
        handler.publisher.publish.assert_not_called()
        delivery.ack.assert_called_once()

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
