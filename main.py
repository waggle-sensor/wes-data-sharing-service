import argparse
import time
import random
import mapper
import pika
import json
import os
import logging


included_fields = ['ts', 'name', 'value', 'plugin']


def on_validator_callback(ch, method, properties, body):
    # Decode intra-node message.
    try:
        msg = json.loads(body)
    except json.JSONDecodeError:
        ch.basic_ack(method.delivery_tag)
        logging.warning('message has bad format %s', body)
        return
    
    scope = msg.get('scope', ['node', 'beehive'])
    
    # Repack JSON with only included_fields and to ensure in compact format.
    try:
        msg = {k: msg[k] for k in included_fields}
    except KeyError:
        ch.basic_ack(method.delivery_tag)
        logging.exception('message missing expected key %s', msg)
        return

    node_body = json.dumps(msg, separators=(',', ':')).encode()

    # The message is valid if it can be mapped to waggle protocol message.
    try:
        beehive_body = mapper.local_to_waggle(msg)
    except Exception:
        ch.basic_ack(method.delivery_tag)
        logging.exception('message could not be serialized %s', msg)
        return

    # Fanout based on scope.
    if 'node' in scope:
        ch.basic_publish('data.topic', msg['name'], node_body)
        logging.debug('node <- %s', node_body)
    if 'beehive' in scope:
        ch.basic_publish('to-beehive', msg['name'], beehive_body)
        logging.debug('beehive <- %s', node_body)
    
    ch.basic_ack(method.delivery_tag)
    logging.info('processed message')


def declare_exchange_with_queue(ch: pika.adapters.blocking_connection.BlockingChannel, name: str):
    ch.exchange_declare(name, exchange_type='fanout', durable=True)
    ch.queue_declare(name, durable=True)
    ch.queue_bind(name, name)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--verbose', action='store_true', help='enable verbose logging')
    parser.add_argument('url', help='rabbitmq server url')
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format='%(asctime)s %(message)s',
        datefmt='%Y/%m/%d %H:%M:%S')
    logging.getLogger('pika').setLevel(logging.CRITICAL)

    logging.info('connecting to rabbitmq server at %s.', args.url)
    connection_parameters = pika.URLParameters(args.url)
    connection = pika.BlockingConnection(connection_parameters)
    channel = connection.channel()

    logging.info('setting up queues and exchanges.')
    declare_exchange_with_queue(channel, 'to-validator')
    declare_exchange_with_queue(channel, 'to-beehive')
    
    logging.info('starting main process.')
    channel.basic_consume('to-validator', on_validator_callback)
    channel.start_consuming()


if __name__ == '__main__':
    main()
