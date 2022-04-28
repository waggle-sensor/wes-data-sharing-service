import argparse
from pathlib import Path
import logging
import json
import time
import kubernetes
import redis


def load_kube_config(kubeconfig: str):
    homeconfig = Path(Path.home(), ".kube/config").absolute()
    if kubeconfig is not None:
        kubernetes.config.load_kube_config(kubeconfig)
    elif homeconfig.exists():
        kubernetes.config.load_kube_config(str(homeconfig))
    else:
        kubernetes.config.load_incluster_config()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true", help="enable verbose logging")
    parser.add_argument("--kubeconfig", default=None, help="kubernetes config")
    parser.add_argument("--watch-timeout-seconds", type=int, default="60", help="timeout seconds for watch")
    parser.add_argument("--watch-label-selector", default="sagecontinuum.org/plugin-task", help="label selector for watch")
    args = parser.parse_args()

    logger = logging.getLogger()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(message)s",
        datefmt="%Y/%m/%d %H:%M:%S",
    )

    load_kube_config(args.kubeconfig)

    client = redis.Redis()

    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("initial cache size is %d", len(client.keys("pod.*")))

    logger.info("starting watcher")

    while True:
        try:
            v1 = kubernetes.client.CoreV1Api()
            watch = kubernetes.watch.Watch()

            for event in watch.stream(v1.list_pod_for_all_namespaces, timeout_seconds=args.watch_timeout_seconds, label_selector=args.watch_label_selector):
                pod = event["object"]
                
                # we only want to cache pod info once it's been assigned to a node
                if pod.spec.node_name is None:
                    continue

                data = {
                    "uid": pod.metadata.uid,
                    "name": pod.metadata.name,
                    "labels": pod.metadata.labels,
                    # "image": pod.spec.containers[0].image.split("/")[-1],
                    "image": pod.spec.containers[0].image,
                    "host": pod.spec.node_name,
                }

                logger.info("caching pod info for %s", data["name"])
                client.set(f"pod.{data['uid']}", json.dumps(data, separators=(",", ":")))

                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug("cache size is %d", len(client.keys("pod.*")))
        except kubernetes.client.exceptions.ApiException:
            logging.exception("watcher received an exception. restarting watcher...")
            time.sleep(3.0)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
