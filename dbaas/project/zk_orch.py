from flask import Flask, request, jsonify, make_response, redirect
import pymongo
import requests
from datetime import datetime
import json
import csv
import sys
import time
import pika
import uuid

class RpcClient(object):
	def __init__(self):
		self.connection = pika.BlockingConnection(
			pika.ConnectionParameters(host='rmq'))

		self.channel = self.connection.channel()

		result = self.channel.queue_declare(queue='responseQ')
		self.callback_queue = result.method.queue

		self.channel.basic_consume(
			queue=self.callback_queue,
			on_message_callback=self.on_response,
			auto_ack=True)

	def on_response(self, ch, method, props, body):
		if self.corr_id == props.correlation_id:
			# ch.basic_ack(delivery_tag=method.delivery_tag)
			self.response = body

	def read_call(self, params):
		self.response = None
		self.corr_id = str(uuid.uuid4())
		self.channel.basic_publish(
			exchange='',
			routing_key='readQ',
			properties=pika.BasicProperties(
				reply_to=self.callback_queue,
				correlation_id=self.corr_id,
			),
			body=params
		)
		while self.response is None:
			self.connection.process_data_events()
		return self.response

	def write_call(self, params):
		self.response = None
		self.corr_id = str(uuid.uuid4())
		self.channel.basic_publish(
			exchange='',
			routing_key='writeQ',
			properties=pika.BasicProperties(
				reply_to=self.callback_queue,
				correlation_id=self.corr_id,
			),
			body=params
		)
		while self.response is None:
			self.connection.process_data_events()
		return self.response


rpc_client = RpcClient()



app = Flask(__name__)


#api 8
@app.route('/api/v1/db/read', methods=['GET'])
def db_read():
	# rides-start
	if request.args.get('ORIGIN') == "RIDE":
		if request.args.get('COMMAND') == "Upcoming":
			source = request.args.get('source')
			destination = request.args.get('destination')
			message = rpc_client.read_call("get_upcoming_rides:" + str(source) + "," + str(destination))
			message = json.loads(message.decode())["message"]
			if message == []:
				return make_response('',204)
			return json.dumps({'upcoming':message}),200

		if request.args.get('COMMAND') == "EXISTS":
			collection_name = request.args.get("COLLECTION")
			val = request.args.get('VALUE')
			field = str(request.args.get("FIELD"))
			message = rpc_client.read_call("entry_exists:" + str(collection_name) + "," + str(field) + "," + str(val)).decode()
			return message, 200

		if request.args.get('COMMAND') == "Ride_Details":
			id = request.args.get('id')
			message = rpc_client.read_call("get_ride_details:" + str(id)).decode()
			return message, 200
		
		if request.args.get('COMMAND') == "READ_REQUEST_COUNT":
			message = rpc_client.read_call("read_request_count_ride:").decode()
			return message, 200

		if request.args.get('COMMAND') == "READ_RIDE_COUNT":
			message = rpc_client.read_call("read_ride_count:").decode()
			return message, 200

	elif request.args.get('ORIGIN') == "USER":
		if request.args.get('COMMAND') == "EXISTS":
			collection_name = request.args.get("DB")
			val = request.args.get('VALUE')
			field = str(request.args.get("FIELD"))
			message = rpc_client.read_call("entry_exists:" + str(collection_name) + "," + str(field) + "," + str(val)).decode()
			return message, 200

		if request.args.get('COMMAND') == "READ_ALL":
			message = rpc_client.read_call("read_all_users:").decode()
			if (message == "0"):
				return make_response('',204)
			else:
				return message, 200

		if request.args.get('COMMAND') == "READ_REQUEST_COUNT":
			message = rpc_client.read_call("read_request_count_user:").decode()
			return message, 200


# api 9
@app.route('/api/v1/db/write', methods=['POST'])
def db_write():
	req = json.loads(request.data)
	# rides-start
	if (req['ORIGIN'] == 'RIDE'):
		if (req['COMMAND'] == 'INSERT'):
			collection_name = req['COLLECTION']
			fields = req['FIELDS']
			data = {}
			for field in range(len(fields)):
				data[fields[field]] = req["VALUES"][field]
			params = json.dumps({"func":"create_entry", "collection":collection_name, "data":data}).encode()
			message = rpc_client.write_call(params).decode()
			if(message == "1"):
				return make_response("",201)

		if(req['COMMAND'] == 'DELETE'):
			collection_name = req['COLLECTION']
			data = {
				req['FIELD']:req['VALUE']
			}
			params = json.dumps({"func":"delete_entry", "collection":collection_name, "data":data}).encode()
			message = rpc_client.write_call(params).decode()
			if(message == "1"):
				return make_response("",201)

		if(req['COMMAND'] == 'Update_Ride'):
			username = req['username']
			id = int(req['id'])
			params = json.dumps({"func":"update_ride", "username":username, "id":id}).encode()
			message = rpc_client.write_call(params).decode()
			if(message == "1"):
				return make_response("",200)
			else:
				return make_response("",400)

		if (req['COMMAND'] == "DELETE_ALL"):
			params = json.dumps({"func":"delete_all", "collection":"Rides"}).encode()
			message = rpc_client.write_call(params).decode()
			if(message == "1"):
				return make_response("",200)

		if (req['COMMAND'] == "RESET_REQUEST_COUNT"):
			params = json.dumps({"func":"reset_request_count_ride"}).encode()
			message = rpc_client.write_call(params).decode()
			if(message == "1"):
				return make_response("",200)

		if (req['COMMAND'] == "ADD_REQUEST_COUNT"):
			params = json.dumps({"func":"add_request_count_ride"}).encode()
			message = rpc_client.write_call(params).decode()
			if(message == "1"):
				return make_response("",200)

		if (req['COMMAND'] == "ADD_RIDE_COUNT"):
			params = json.dumps({"func":"add_ride_count"}).encode()
			message = rpc_client.write_call(params).decode()
			if(message == "1"):
				return make_response("",200)


	if (req["ORIGIN"] == "USER"):
		if (req['COMMAND'] == 'INSERT'):
			collection = req['DB']
			fields = req['FIELDS']
			data = {}
			for field in range(len(fields)):
				data[fields[field]] = req["VALUES"][field]
			params = json.dumps({"func":"create_entry", "collection":collection, "data":data}).encode()
			message = rpc_client.write_call(params).decode()
			if(message == "1"):
				return make_response("",201)

		if(req['COMMAND'] == 'DELETE'):
			collection = req['DB']
			data = {
				req['FIELD']:req['VALUE']
			}
			params = json.dumps({"func":"delete_entry", "collection":collection, "data":data}).encode()
			message = rpc_client.write_call(params).decode()
			if(message == "1"):
				return make_response("",200)

		if (req['COMMAND'] == "DELETE_ALL"):
			params = json.dumps({"func":"delete_all", "collection":"Users"}).encode()
			message = rpc_client.write_call(params).decode()
			if(message == "1"):
				return make_response("",200)

		if (req['COMMAND'] == "RESET_REQUEST_COUNT"):
			params = json.dumps({"func":"reset_request_count_user"}).encode()
			message = rpc_client.write_call(params).decode()
			if(message == "1"):
				return make_response("",200)

		if (req['COMMAND'] == "ADD_REQUEST_COUNT"):
			params = json.dumps({"func":"add_request_count_user"}).encode()
			message = rpc_client.write_call(params).decode()
			if(message == "1"):
				return make_response("",200)


if __name__ == '__main__':
	app.run(host='0.0.0.0', debug = False)