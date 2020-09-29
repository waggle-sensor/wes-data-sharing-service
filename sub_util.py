import time
import random
import mapper
import pika

def on_local_callback(ch, method, properties, body):
    print(body)

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        credentials=pika.PlainCredentials('worker', 'worker'),
    ))
    channel = connection.channel()

    queue = channel.queue_declare('', exclusive=True).method.queue
    channel.queue_bind(queue=queue, exchange='data.topic', routing_key='env.temperature.#')
    channel.basic_consume(queue, on_local_callback, auto_ack=True)
    channel.start_consuming()


if __name__ == '__main__':
    main()
