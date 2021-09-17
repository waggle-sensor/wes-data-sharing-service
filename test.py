import unittest
import wagglemsg
import pika
from waggle import message
from kubernetes.client import V1Pod, V1PodSpec, V1ObjectMeta, V1Container
from main import AppState, match_plugin_user_id, load_message, InvalidMessageError

def get_test_appstate():
    podlist = [
        V1Pod(
            api_version="v1",
            kind="Pod",
            metadata=V1ObjectMeta(
                uid="9a28e690-ad5d-4027-90b3-1da2b41cf4d1",
                labels={}
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
        ),
        V1Pod(
            api_version="v1",
            kind="Pod",
            metadata=V1ObjectMeta(
                uid="c3100d9b-2262-47ac-ab38-553862791174",
                labels={
                    "sagecontinuum.org/plugin-job": "sampler-job",
                    "sagecontinuum.org/plugin-task": "imagesampler-left",
                }
            ),
            spec=V1PodSpec(
                node_name="nxcore",
                containers=[
                    V1Container(
                        name="plugin-image-sampler-0-2-1-0953bd15-d7947ddbd-89f2m",
                        image="docker.io/waggle/plugin-imagesampler:0.2.1",
                    )
                ],
            ),
        )
    ]

    return AppState(
        node="111111222222333333",
        vsn="W123",
        pods={pod.metadata.uid: pod for pod in podlist},
)


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
            user_id="plugin.plugin-iio:1.2.3",
        )
        body = wagglemsg.dump(wagglemsg.Message(
            timestamp=1360287003083988472,
            name="test",
            value=23.1,
            meta={
                "sensor": "bme280",
            }
        ))

        msg = load_message(get_test_appstate(), properties, body)

        # TODO eventually the contract should be that plugin comes from the *image*. so, the test would be:
        # plugin = waggle/plugin-iio:0.2.0
        # but, for now, we'll maintain backwards compatibility.
        self.assertDictEqual(msg.meta, {
            "node": "111111222222333333",
            "vsn": "W123",
            "host": "rpi-node",
            "job": "sage",
            "task": "plugin-iio-1.2.3",
            "sensor": "bme280",
            "plugin": "plugin-iio:1.2.3",
        })

    def test_load_message_job_and_task(self):
        properties = pika.BasicProperties(
            app_id="c3100d9b-2262-47ac-ab38-553862791174",
            user_id="plugin.plugin-imagesampler-0-2-1-abcef103",
        )
        body = wagglemsg.dump(wagglemsg.Message(
            timestamp=1360287003083988472,
            name="test",
            value=23.1,
            meta={
                "camera": "bottom",
            }
        ))

        msg = load_message(get_test_appstate(), properties, body)

        self.assertDictEqual(msg.meta, {
            "node": "111111222222333333",
            "vsn": "W123",
            "host": "nxcore",
            "job": "sampler-job",
            "task": "imagesampler-left",
            "camera": "bottom",
            "plugin": "plugin-imagesampler:0.2.1",
        })

    def test_load_message_invalid_uid(self):
        properties = pika.BasicProperties(
            app_id="non-existant",
            user_id="plugin.plugin-metsense:1.2.3",
        )
        body = wagglemsg.dump(wagglemsg.Message(
            timestamp=1360287003083988472,
            name="test",
            value=23.1,
            meta={
                "camera": "left",
            }
        ))
        with self.assertRaises(InvalidMessageError):
            load_message(get_test_appstate(), properties, body)

    def test_load_message_backwards_compatible(self):
        properties = pika.BasicProperties(
            user_id="plugin.plugin-metsense:1.2.3",
        )
        body = wagglemsg.dump(wagglemsg.Message(
            timestamp=1360287003083988472,
            name="test",
            value=23.1,
            meta={
                "camera": "left",
            }
        ))
        msg = load_message(get_test_appstate(), properties, body)
        self.assertNotIn("host", msg.meta)

    def test_load_message_upload(self):
        properties = pika.BasicProperties(
            app_id="9a28e690-ad5d-4027-90b3-1da2b41cf4d1",
            user_id="plugin.plugin-metsense:1.2.3",
        )
        body = wagglemsg.dump(wagglemsg.Message(
            timestamp=1360287003083988472,
            name="upload",
            value=23.1,
            meta={
                "camera": "left",
                "filename": "sample.jpg"
            }
        ))

        msg = load_message(get_test_appstate(), properties, body)
        self.assertEqual(msg.value, "https://storage.sagecontinuum.org/api/v1/data/sage/plugin-metsense-1.2.3/111111222222333333/1360287003083988472-sample.jpg")

    def test_load_message_upload_raise_missing_filename(self):
        properties = pika.BasicProperties(
            app_id="9a28e690-ad5d-4027-90b3-1da2b41cf4d1",
            user_id="plugin.plugin-metsense:1.2.3",
        )
        body = wagglemsg.dump(wagglemsg.Message(
            timestamp=1360287003083988472,
            name="upload",
            value=23.1,
            meta={
                "camera": "left",
            }
        ))

        with self.assertRaises(InvalidMessageError):
            load_message(get_test_appstate(), properties, body)

if __name__ == "__main__":
    unittest.main()
