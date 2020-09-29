import time
import random
import mapper
import pika
import msgpack
import json
import zlib

def on_validator_callback(ch, method, properties, body):
    # The message is valid if it can be mapped to a waggle protocol message.
    # This ensures that the same kinds of messages are available everywhere.
    try:
        local_msg = json.loads(body)
        waggle_body = mapper.local_to_waggle(local_msg)
    except Exception as exc:
        print('invalid msg', body, exc)
        ch.basic_ack(method.delivery_tag)
        return

    ch.basic_publish('data.topic', local_msg['topic'], body)
    ch.basic_publish('to-beehive', method.routing_key, waggle_body)
    ch.basic_ack(method.delivery_tag)
    print('send to local', len(body), len(zlib.compress(body)), body)


def on_beehive_callback(ch, method, properties, body):
    ch.basic_ack(method.delivery_tag)
    print('send to behive', len(body), body)


def declare_exchange_with_queue(ch: pika.adapters.blocking_connection.BlockingChannel, name: str):
    ch.exchange_declare(name, exchange_type='fanout', durable=True)
    ch.queue_declare(name, durable=True)
    ch.queue_bind(name, name)


def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        credentials=pika.PlainCredentials('worker', 'worker'),
    ))
    channel = connection.channel()

    declare_exchange_with_queue(channel, 'to-validator')
    declare_exchange_with_queue(channel, 'to-beehive')

    channel.basic_consume('to-validator', on_validator_callback)
    channel.basic_consume('to-beehive', on_beehive_callback)

    channel.start_consuming()


if __name__ == '__main__':
    main()
