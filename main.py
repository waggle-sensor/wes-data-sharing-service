import time
import random
import mapper
import pika
import json


included_fields = ['ts', 'topic', 'value', 'plugin']


def on_validator_callback(ch, method, properties, body):
    # Decode intra-node message.
    try:
        msg = json.loads(body)
    except json.JSONDecodeError:
        ch.basic_ack(method.delivery_tag)
        print('invalid message', body)
        return
    
    scope = msg.get('scope', ['node', 'beehive'])
    
    # Repack JSON with only included_fields and to ensure in compact format.
    msg = {k: msg[k] for k in included_fields}
    node_body = json.dumps(msg, separators=(',', ':')).encode()

    # The message is valid if it can be mapped to waggle protocol message.
    try:
        beehive_body = mapper.local_to_waggle(msg)
    except Exception:
        ch.basic_ack(method.delivery_tag)
        print('could not serialize message', msg)
        return

    # Fanout based on scope.
    if 'node' in scope:
        ch.basic_publish('data.topic', msg['topic'], node_body)
    if 'beehive' in scope:
        ch.basic_publish('to-beehive', msg['topic'], beehive_body)

    ch.basic_ack(method.delivery_tag)


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
    channel.start_consuming()


if __name__ == '__main__':
    main()
