from dataclasses import dataclass
import pika


@dataclass
class Delivery:

    channel: pika.adapters.blocking_connection.BlockingChannel = None
    delivery_tag: int = 0
    routing_key: str = None
    pod_uid: str = None
    body: bytes = None

    def ack(self):
        self.channel.connection.add_callback_threadsafe(self._ack)

    def nack(self):
        self.channel.connection.add_callback_threadsafe(self._nack)

    def reject(self):
        self.channel.connection.add_callback_threadsafe(self._reject)

    def _ack(self):
        self.channel.basic_ack(self.delivery_tag)

    def _nack(self):
        self.channel.basic_nack(self.delivery_tag)

    def _reject(self):
        self.channel.basic_reject(self.delivery_tag)

    def __str__(self):
        return str(self.delivery_tag)


def consume(channel, queue, handler):
    def pika_handler(ch, method, properties, body):
        handler(Delivery(
            channel=ch,
            delivery_tag=method.delivery_tag,
            routing_key=method.routing_key,
            pod_uid=properties.app_id,
            body=body,
        ))
    channel.basic_consume(queue, pika_handler)


@dataclass
class Publishing:

    body: bytes
    properties: pika.BasicProperties = None


def publish(channel, exchange: str, routing_key: str, publishing: Publishing):
    channel.basic_publish(
        exchange=exchange,
        routing_key=routing_key,
        body=publishing.body,
        properties=publishing.properties,
    )
