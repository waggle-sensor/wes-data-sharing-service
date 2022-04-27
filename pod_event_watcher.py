import kubernetes
from queue import Queue, Empty
from threading import Thread, Event
from dataclasses import dataclass
from prometheus_client import Counter, Gauge
import time
import logging


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

    def __init__(self):
        self.watch = kubernetes.watch.Watch()
        self.events = Queue()
        self.stopped = Event()
        Thread(target=self.main, daemon=True).start()

    def stop(self):
        self.watch.stop()
    
    def is_stopped(self) -> bool:
        return self.stopped.is_set()

    def main(self):
        self.stopped.clear()
        while True:
            try:
                self.logger.info("watching kubernetes pod events")
                self.watch_events()
                self.logger.info("watcher timed out stream. will watch again...")
            except kubernetes.client.exceptions.ApiException:
                self.logger.exception("received kubernetes api exception. will retry...")
                wes_data_service_kubernetes_api_exception_total.inc()
                wes_data_service_kubernetes_last_exception_time.set_to_current_time()
                time.sleep(5.0)
                continue
            except Exception:
                self.stopped.set()
                return

    def watch_events(self):
        v1 = kubernetes.client.CoreV1Api()
        for event in self.watch.stream(v1.list_pod_for_all_namespaces, timeout_seconds=300, label_selector="sagecontinuum.org/plugin-task"):
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
