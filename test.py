import unittest
import wagglemsg
import pika
from waggle import message
from kubernetes.client import V1Pod, V1PodSpec, V1ObjectMeta, V1Container
from main import AppState, load_message, InvalidMessageError


def get_test_state():
    podlist = [
        V1Pod(
            api_version="v1",
            kind="Pod",
            metadata=V1ObjectMeta(
                uid="9a28e690-ad5d-4027-90b3-1da2b41cf4d1",
                labels={
                    "sagecontinuum.org/plugin-task": "iio-rpi",
                }
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
        ),
        V1Pod(
            api_version="v1",
            kind="Pod",
            metadata=V1ObjectMeta(
                uid="b38b6dad-f95f-4b44-a1a0-44f69b333274",
                labels={
                    "sagecontinuum.org/plugin-task": "no-namespace",
                }
            ),
            spec=V1PodSpec(
                node_name="nxcore",
                containers=[
                    V1Container(
                        name="fuzz-name-to-ensure-no-dependency",
                        image="no-namespace:1.2.3",
                    )
                ],
            ),
        ),
    ]

    return AppState(
        node="111111222222333333",
        vsn="W123",
        upload_publish_name="xxx.upload",
        pods={pod.metadata.uid: pod for pod in podlist},
    )


def load_message_for_test_case(app_id, name="test", meta={}, appstate=None):
    """compact way to specify our set of test cases"""
    if appstate is None:
        appstate = get_test_state()

    properties = pika.BasicProperties()
    properties.app_id = app_id

    body = wagglemsg.dump(wagglemsg.Message(
        timestamp=1360287003083988472,
        name=name,
        value=23.1,
        meta=meta.copy()
    ))

    return load_message(appstate, properties, body)


class TestMain(unittest.TestCase):

    def test_load_message(self):
        appstate = get_test_state()

        for uid, pod in appstate.pods.items():
            msg = load_message_for_test_case(
                app_id=uid,
                meta={
                    "user-defined1": "value1",
                    "user-defined2": "value2",
                    "user-defined3": "value3",
                },
            )

            self.assertDictEqual(msg.meta, {
                "node": appstate.node,
                "vsn": appstate.vsn,
                "host": pod.spec.node_name,
                "job": pod.metadata.labels.get("sagecontinuum.org/plugin-job", "sage"),
                "task": pod.metadata.labels["sagecontinuum.org/plugin-task"],
                # plugin must match just the name for now, no namespace
                "plugin": pod.spec.containers[0].image.split("/")[-1],
                "user-defined1": "value1",
                "user-defined2": "value2",
                "user-defined3": "value3",
            })

    def test_load_message_nonexistant_uid(self):
        with self.assertRaises(InvalidMessageError):
            load_message_for_test_case(app_id="non-existant")

    def test_load_message_upload(self):
        appstate = get_test_state()

        for uid, pod in appstate.pods.items():
            msg = load_message_for_test_case(
                app_id=uid,
                name="upload",
                meta={
                    "camera": "left",
                    "filename": "sample.jpg"
                },
                appstate=appstate,
            )
            
            self.assertEqual(msg.name, appstate.upload_publish_name)

            job = pod.metadata.labels.get("sagecontinuum.org/plugin-job", "sage")
            task = pod.metadata.labels["sagecontinuum.org/plugin-task"]
            version = pod.spec.containers[0].image.split(":")[-1]
            self.assertEqual(msg.value, f"https://storage.sagecontinuum.org/api/v1/data/{job}/sage-{task}-{version}/{appstate.node}/1360287003083988472-sample.jpg")

    def test_load_message_upload_raise_missing_filename(self):
        appstate = get_test_state()

        for uid in appstate.pods.keys():
            with self.assertRaises(InvalidMessageError):
                load_message_for_test_case(
                    app_id=uid,
                    name="upload",
                    meta={
                        "camera": "left",
                    }
                )

if __name__ == "__main__":
    unittest.main()
