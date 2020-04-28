import pika
import sys

message = ' '.join(sys.argv[1:]) or "Hello World!"

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost')
)

channel = connection.channel()

channel.queue_declare(queue='hello', auto_delete=True)

channel.basic_publish(
    exchange='',
    routing_key='hello',
    body=message
)

print(" [x] Sent %r"%message)
connection.close()