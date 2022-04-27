import kubernetes
from queue import Queue, Empty
from threading import Thread, Event
from dataclasses import dataclass
from prometheus_client import Counter, Gauge
import time
import logging
from uuid import uuid4


wes_data_service_kubernetes_pod_events_total = Counter("wes_data_service_kubernetes_pod_events_total", "Total number of Kubernetes pod events received.")
wes_data_service_kubernetes_api_exception_total = Counter("wes_data_service_kubernetes_api_exception_total", "Total number of Kubernetes API exceptions.")
wes_data_service_kubernetes_last_exception_time = Gauge("wes_data_service_kubernetes_last_exception_time", "Last exception time as UNIX timestamp.")


@dataclass
class Pod:

    uid: str = None
    name: str = None
    labels: dict = None
    image: str = None
    host: str = None


class PluginPodEventWatcher:

    logger = logging.getLogger("PluginPodEventWatcher")

    def __init__(self, timeout_seconds=30, label_selector="sagecontinuum.org/plugin-task"):
        self.uuid = uuid4()
        self.timeout_seconds = timeout_seconds
        self.label_selector = label_selector
        self.run = Event()
        self.stop = Event()
        self.events = Queue()

    def __enter__(self):
        if self.run.is_set():
            raise RuntimeError("watcher already run")
        self.run.set()
        Thread(target=self.main, daemon=True).start()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.stop.set()

    def main(self):
        self.logger.info("pod watcher %s started", self.uuid)
        try:
            while not self.stop.is_set():
                try:
                    self.watch_events()
                except kubernetes.client.exceptions.ApiException:
                    self.logger.exception("pod watcher %s received api exception. will retry...", self.uuid)
                    wes_data_service_kubernetes_api_exception_total.inc()
                    wes_data_service_kubernetes_last_exception_time.set_to_current_time()
                    time.sleep(5.0)
        except Exception:
            self.logger.exception("pod watcher %s had unhandled exception", self.uuid)
        finally:
            self.events.put(None)
            self.logger.info("pod watcher %s stopped", self.uuid)

    def watch_events(self):
        v1 = kubernetes.client.CoreV1Api()
        watch = kubernetes.watch.Watch()

        self.logger.debug("pod watcher %s watching events", self.uuid)
        for event in watch.stream(v1.list_pod_for_all_namespaces, timeout_seconds=self.timeout_seconds, label_selector=self.label_selector):
            if self.stop.is_set():
                watch.stop()
                return
            pod = event["object"]
            if pod.spec.node_name is None:
                continue
            self.events.put(Pod(
                uid=pod.metadata.uid,
                name=pod.metadata.name,
                labels=pod.metadata.labels,
                image=pod.spec.containers[0].image.split("/")[-1],
                host=pod.spec.node_name,
            ))
            wes_data_service_kubernetes_pod_events_total.inc()

    def ready_events(self):
        while True:
            try:
                yield self.events.get_nowait()
            except Empty:
                break
