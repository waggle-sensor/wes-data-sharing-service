import argparse
import time
import random
import mapper
import pika
import json
import os
import logging


def on_validator_callback(ch, method, properties, body):
    try:
        msg = json.loads(body)
    except json.JSONDecodeError:
        logging.warning('message has bad format %s', body)
        return

    scope = msg.get('scope', ['node', 'beehive'])

    try:
        msg = extract_items(msg, ['ts', 'name', 'value', 'plugin'])
    except KeyError:
        logging.warning('message missing expected keys %s', msg)
        return

    if 'node' in scope:
        publish_to_node(ch, msg)
    if 'beehive' in scope:
        publish_to_beehive(ch, msg)
    logging.info('processed message')


def extract_items(d, ks):
    return {k: d[k] for k in ks}


def publish_to_node(ch, msg):
    body = json.dumps(msg, separators=(',', ':')).encode()
    ch.basic_publish('data.topic', msg['name'], body)
    logging.debug('node <- %s', body)


def publish_to_beehive(ch, msg):
    try:
        body = mapper.local_to_waggle(msg)
    except Exception:
        logging.warning('message could not be serialized %s', msg)
        return
    ch.basic_publish('to-beehive', msg['name'], body)
    logging.debug('beehive <- %s', body)


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
    # pika logging is too verbose, so we turn it down.
    logging.getLogger('pika').setLevel(logging.CRITICAL)

    logging.info('connecting to rabbitmq server at %s.', args.url)
    connection_parameters = pika.URLParameters(args.url)
    connection = pika.BlockingConnection(connection_parameters)
    channel = connection.channel()

    logging.info('setting up queues and exchanges.')
    declare_exchange_with_queue(channel, 'to-validator')
    declare_exchange_with_queue(channel, 'to-beehive')
    
    logging.info('starting main process.')
    channel.basic_consume('to-validator', on_validator_callback, auto_ack=True)
    channel.start_consuming()


if __name__ == '__main__':
    main()
