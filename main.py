import argparse
import time
import random
import mapper
import pika
import json
import os
import logging
from hashlib import sha1

WAGGLE_NODE_ID = os.environ.get('WAGGLE_NODE_ID', '0000000000000000')

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq-server')
RABBITMQ_PORT = int(os.environ.get('RABBITMQ_PORT', '5672'))
RABBITMQ_USERNAME = os.environ.get('RABBITMQ_USERNAME', 'service')
RABBITMQ_PASSWORD = os.environ.get('RABBITMQ_PASSWORD', 'service')

def on_validator_callback(ch, method, properties, body):
    logging.info('processing message')
    if properties.type is None:
        logging.warning('message missing type')
        return
    if properties.timestamp is None:
        logging.warning('message missing timestamp')
        return
    if properties.reply_to is None:
        logging.warning('message missing reply_to')
        return
    if method.routing_key in ['node', 'all']:
        publish_to_node(ch, properties, body)
    if method.routing_key in ['beehive', 'all']:
        publish_to_beehive(ch, properties, body)
    logging.info('processed message')


def publish_to_node(ch, properties, body):
    ch.basic_publish(
        exchange='data.topic',
        routing_key=properties.type,
        properties=properties,
        body=body)
    logging.debug('node <- %s', body)


def publish_to_beehive(ch, properties, body):
    if properties.content_type is None:
        value = body
    elif properties.content_type == 'application/json':
        value = json.loads(body)
    elif properties.content_type == 'image/png':
        value = sha1(body).hexdigest()
        logging.info('staging image for large file transport with ref %s', value)

    msg = {
        'ts': properties.timestamp,
        'name': properties.type,
        'plugin': properties.reply_to,
        'value': value,
    }

    try:
        body = mapper.local_to_waggle(msg)
    except Exception:
        logging.warning('message could not be serialized %s', msg)
        return

    # tag until we work out plan for this
    properties.content_type = 'application/waggle'

    # add self to route
    properties.reply_to = '.'.join([WAGGLE_NODE_ID, properties.reply_to])

    ch.basic_publish(
        exchange='to-beehive',
        routing_key=properties.type,
        properties=properties,
        body=body)
    logging.debug('beehive <- %s', body)


def declare_exchange_with_queue(ch: pika.adapters.blocking_connection.BlockingChannel, name: str):
    ch.exchange_declare(name, exchange_type='fanout', durable=True)
    ch.queue_declare(name, durable=True)
    ch.queue_bind(name, name)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--verbose', action='store_true', help='enable verbose logging')
    parser.add_argument('--rabbitmq-host', default=RABBITMQ_HOST, help='rabbitmq host')
    parser.add_argument('--rabbitmq-port', default=RABBITMQ_PORT, type=int, help='rabbitmq port')
    parser.add_argument('--rabbitmq-username', default=RABBITMQ_USERNAME, help='rabbitmq username')
    parser.add_argument('--rabbitmq-password', default=RABBITMQ_PASSWORD, help='rabbitmq password')
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format='%(asctime)s %(message)s',
        datefmt='%Y/%m/%d %H:%M:%S')
    # pika logging is too verbose, so we turn it down.
    logging.getLogger('pika').setLevel(logging.CRITICAL)

    params = pika.ConnectionParameters(
        host=args.rabbitmq_host,
        port=args.rabbitmq_port,
        credentials=pika.PlainCredentials(
            username=args.rabbitmq_username,
            password=args.rabbitmq_password,
        ),
        client_properties={'name': 'data-sharing-service'},
    )

    logging.info('connecting to rabbitmq server at %s:%d as %s.', params.host, params.port, params.credentials.username)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    logging.info('setting up queues and exchanges.')
    channel.exchange_declare('data.topic', exchange_type='topic', durable=True)
    declare_exchange_with_queue(channel, 'to-validator')
    declare_exchange_with_queue(channel, 'to-beehive')
    
    logging.info('starting main process.')
    channel.basic_consume('to-validator', on_validator_callback, auto_ack=True)
    channel.start_consuming()


if __name__ == '__main__':
    main()
