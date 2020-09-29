import argparse
import time
import random
import mapper
import pika

def on_local_callback(ch, method, properties, body):
    print(body)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('topics', nargs='+')
    args = parser.parse_args()

    connection = pika.BlockingConnection(pika.ConnectionParameters(
        credentials=pika.PlainCredentials('worker', 'worker'),
    ))
    channel = connection.channel()

    queue = channel.queue_declare('', exclusive=True).method.queue

    for topic in args.topics:
        channel.queue_bind(queue=queue, exchange='data.topic', routing_key=topic)

    channel.basic_consume(queue, on_local_callback, auto_ack=True)
    channel.start_consuming()


if __name__ == '__main__':
    main()
