#!/usr/bin/env python
import pika
import sys

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='hello', auto_delete=True, durable=True)
channel.exchange_declare(exchange='logs', exchange_type='fanout', auto_delete=True)
channel.queue_bind(exchange='logs', queue='hello')

message = ' '.join(sys.argv[1:]) or "info: Hello World! ............"
channel.basic_publish(exchange='logs', routing_key='hello', body=message)
print(" [x] Sent %r" % message)
a = input("enter text: ")
print(a)
connection.close()