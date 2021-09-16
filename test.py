import unittest
import pika
from waggle import message
from kubernetes.client import V1Pod, V1PodSpec, V1ObjectMeta, V1Container
from main import match_plugin_user_id, load_message, InvalidMessageError


test_pods_list = [
    V1Pod(
        api_version="v1",
        kind="Pod",
        metadata=V1ObjectMeta(
            uid="9a28e690-ad5d-4027-90b3-1da2b41cf4d1",
            labels={"role": "plugin"}
        ),
        spec=V1PodSpec(
            node_name="rpi-node",
            containers=[
                V1Container(
                    name="plugin-iio-0-2-0-4c07bb56",
                    image="waggle/plugin-iio:0.2.0",
                )
            ],
        ),
    )
]

test_pods = {pod.metadata.uid: pod for pod in test_pods_list}


class TestMain(unittest.TestCase):

    def test_match_plugin_user_id(self):
        tests = [
            ("plugin.plugin-metsense:1.2.3", "plugin-metsense:1.2.3"),
            ("plugin.plugin-raingauge-1-2-3-ae43fc12", "plugin-raingauge:1.2.3"),
            ("plugin.hello-world-4-0-2-aabbccdd", "hello-world:4.0.2"),
        ]

        for user_id, want in tests:
            self.assertEqual(match_plugin_user_id(user_id), want)
    
    def test_load_message(self):
        properties = pika.BasicProperties(
            app_id="9a28e690-ad5d-4027-90b3-1da2b41cf4d1",
            user_id="plugin.plugin-metsense:1.2.3",
        )
        body = message.dump(message.Message(
            timestamp=1360287003083988472,
            name="test",
            value=23.1,
            meta={
                "camera": "left",
            }
        ))

        msg = load_message(test_pods.copy(), properties, body)
        self.assertEqual(msg.meta["host"], "rpi-node")
        # TODO eventually the contract should be that plugin comes from the *image*. so, the test would be:
        # self.assertEqual(msg.meta["plugin"], "waggle/plugin-iio:0.2.0")
        # for now, we'll maintain backwards compatibility:
        self.assertEqual(msg.meta["plugin"], "plugin-metsense:1.2.3")

    def test_load_message_invalid_uid(self):
        properties = pika.BasicProperties(
            app_id="non-existant",
            user_id="plugin.plugin-metsense:1.2.3",
        )
        body = message.dump(message.Message(
            timestamp=1360287003083988472,
            name="test",
            value=23.1,
            meta={
                "camera": "left",
            }
        ))
        with self.assertRaises(InvalidMessageError):
            load_message(test_pods.copy(), properties, body)

    def test_load_message_backwards_compatible(self):
        properties = pika.BasicProperties(
            user_id="plugin.plugin-metsense:1.2.3",
        )
        body = message.dump(message.Message(
            timestamp=1360287003083988472,
            name="test",
            value=23.1,
            meta={
                "camera": "left",
            }
        ))
        msg = load_message(test_pods.copy(), properties, body)
        self.assertNotIn("host", msg.meta)

if __name__ == "__main__":
    unittest.main()
