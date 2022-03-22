import kubernetes
from queue import Queue, Empty
from threading import Thread, Event
from prometheus_client import Counter


wes_data_service_pod_events_total = Counter("wes_data_service_pod_events_total", "Total number of pod events received.")


class PluginPodEventWatcher:

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
        try:
            self.watch_events()
        finally:
            self.stopped.set()

    def watch_events(self):
        v1 = kubernetes.client.CoreV1Api()
        for event in self.watch.stream(v1.list_pod_for_all_namespaces, label_selector="sagecontinuum.org/plugin-task"):
            pod = event["object"]
            if pod.spec.node_name is None:
                continue
            self.events.put(pod)
            wes_data_service_pod_events_total.inc()

    def ready_events(self):
        while True:
            try:
                yield self.events.get_nowait()
            except Empty:
                break
