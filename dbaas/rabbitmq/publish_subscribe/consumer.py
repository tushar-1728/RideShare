#!/usr/bin/env python
import pika
import sys
import time

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='logs', exchange_type='fanout', auto_delete=True)

channel.queue_declare(queue='hello', auto_delete=True, durable=True)

channel.queue_bind(exchange='logs', queue='hello')

print(' [*] Waiting for logs. To exit press CTRL+C')

def callback(channel, method, properties, body):
    print(" [x] Received %r" % body)
    time.sleep( body.count(b'.') )
    print(" [x] Done")
    channel.basic_ack(delivery_tag = method.delivery_tag)
    channel.basic_publish(exchange='logs', routing_key='hello', body=body)

channel.basic_consume(
    queue='hello',
    on_message_callback=callback
)

channel.start_consuming()