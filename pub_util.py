import time
import random
import mapper
import pika
import json
import base64

def pub(ch, msg):
    print('pub', msg)
    ch.basic_publish('to-validator', '', json.dumps(msg, separators=(',', ':')))

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        credentials=pika.PlainCredentials('plugin', 'plugin'),
    ))
    channel = connection.channel()

    while True:
        pub(channel, {
            'ts': 0,
            'value': base64.b64encode(bytes([1, 2, 3, 4, 5, 6])).decode(),
            'plugin': 'simple:0.1.0',
            'topic': 'raw.tmp112',
        })

        pub(channel, {
            'ts': 0,
            'value': 23.1,
            'plugin': 'simple:0.1.0',
            'topic': 'env.temperature.htu21d',
        })

        pub(channel, {
            'ts': 0,
            'value': 22.9,
            'plugin': 'simple:0.1.0',
            'topic': 'env.temperature.tmp112',
        })

        pub(channel, {
            'ts': 0,
            'value': 80.2,
            'plugin': 'simple:0.1.0',
            'topic': 'env.humidity.htu21d',
        })

        time.sleep(1)

if __name__ == '__main__':
    main()
