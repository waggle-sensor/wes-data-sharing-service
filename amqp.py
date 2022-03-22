import pika
from dataclasses import dataclass


@dataclass
class Delivery:
    
    channel: pika.adapters.blocking_connection.BlockingChannel
    method: object
    properties: pika.BasicProperties
    body: bytes

    # TODO(sean) we could make this threadsafe using the connection callback

    def ack(self):
        self.channel.basic_ack(self.method.delivery_tag)

    def nack(self):
        self.channel.basic_nack(self.method.delivery_tag)

    def reject(self):
        self.channel.basic_reject(self.method.delivery_tag)

    def __str__(self):
        return str(self.method.delivery_tag)


def consume(channel, queue, handler):
    def pika_handler(ch, method, properties, body):
        handler(Delivery(
            channel=ch,
            method=method,
            properties=properties,
            body=body,
        ))
    channel.basic_consume(queue, pika_handler)
