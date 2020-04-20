from flask import Flask, request, jsonify, make_response, redirect
import pymongo
import requests
from datetime import datetime
import json
import csv
import sys
import time
import pika

# connection = pika.BlockingConnection(
# 	pika.ConnectionParameters(host='rmq'))
# channel = connection.channel()

app = Flask(__name__)

def dbState(collection):
	try:
		myclient = pymongo.MongoClient('mongodb://rides_mongodb:27017/')
	except:
		return False,False
	db = myclient["RideShare"]
	collection = db[collection]
	return collection


def Add_area():
	myclient = pymongo.MongoClient('mongodb://rides_mongodb:27017/')
	db = myclient["RideShare"]
	collection = db["Area"]
	with open(r"AreaNameEnum.csv","r") as f:
		readCSV = list(csv.DictReader(f))
		for i in range(0, len(readCSV)):
			readCSV[i]['_id'] = int(readCSV[i]['_id'])
			readCSV[i]['Area No'] = int(readCSV[i]['Area No'])
		collection.insert_many(readCSV)


def get_upcoming_rides(source, destination):
	message = []
	collection = dbState('Rides')
	for rides in collection.find({"source":source,"destination":destination},{"_id":1,"created_by":1,"timestamp":1}):
		if datetime.strptime(datetime.now().strftime("%d-%m-%Y:%S-%M-%H"),"%d-%m-%Y:%S-%M-%H") < datetime.strptime(rides["timestamp"],"%d-%m-%Y:%S-%M-%H"):
			rides = {
				"rideId": rides["_id"],
				"username": rides["created_by"],
				"timestamp":rides["timestamp"]
			}
			message.append(rides)
	return message


def entry_exists(collection_name, field, val):
	collection = dbState(collection_name)
	try:
		val = int(val)
	except:
		val = str(val)
	data = {field: val}
	return json.dumps({"count":collection.count_documents(data)})


def get_ride_details(id):
	collection = dbState('Rides')
	return json.dumps(collection.find_one({'_id':int(id)}))

def read_requet_count_ride():
	collection = dbState("RideCount")
	count = collection.find_one({"_id" : "request"})["count"]
	return json.dumps({"count":count})


def read_ride_count():
	collection = dbState("RideCount")
	count = collection.find_one({"_id" : "ride"})["count"]
	return json.dumps({"count":count})


def create_ride(collection_name, data):
	collection = dbState(collection_name)
	if collection.count_documents({"_id":"Last_Id"}) == 0:
		collection.insert_one({"_id":"Last_Id","Last_Id":0})
	Last_Id = collection.find_one({"_id":"Last_Id"})["Last_Id"] + 1
	data["_id"] = Last_Id
	collection.insert_one(data)
	collection.update_one({"_id":"Last_Id" }, {"$set" : {"Last_Id":Last_Id}})
	return True


def delete_ride(collection_name, data):
	collection = dbState(collection_name)
	collection.delete_one(data)
	return True


def update_ride(username, id):
	collection = dbState('Rides')
	message = collection.find_one({"_id":id})
	if username not in message['users']:
		if datetime.strptime(datetime.now().strftime("%d-%m-%Y:%S-%M-%H"),"%d-%m-%Y:%S-%M-%H") < datetime.strptime(message["timestamp"],"%d-%m-%Y:%S-%M-%H"):
			message['users'].append(username)
		else:
			return False
	else:
		return False
	collection.update_one({"_id":id}, {"$set" : {"users":message["users"]}})
	return True


def delete_all_ride():
	collection = dbState('Rides')
	collection.remove({})
	return True


def delete_request_count_ride():
	collection = dbState("RideCount")
	collection.update_one({'_id': "request"}, {'$set': {'count': 0}})
	return True


def add_request_count_ride():
	collection = dbState("RideCount")
	collection.update_one({'_id': "request"}, {'$inc': {'count': 1}})
	return True


def add_ride_count():
	collection = dbState("RideCount")
	collection.update_one({'_id': "ride"}, {'$inc': {'count': 1}})
	return True


#api 8
@app.route('/api/v1/db/read', methods=['GET'])
def db_read():
	# rides-start
	if request.args.get('ORIGIN') == "RIDE":
		if request.args.get('COMMAND') == "Upcoming":
			source = request.args.get('source')
			destination = request.args.get('destination')
			message = get_upcoming_rides(source, destination)
			if message == []:
				return make_response('',204)
			return json.dumps({'upcoming':message}),200

		if request.args.get('COMMAND') == "EXISTS":
			collection_name = request.args.get("COLLECTION")
			field = str(request.args.get("FIELD"))
			val = request.args.get('VALUE')
			message = entry_exists(collection_name, field, val)
			return message, 200

		if request.args.get('COMMAND') == "Ride_Details":
			id = request.args.get('id')
			message = get_ride_details(id)
			return message, 200
		
		if request.args.get('COMMAND') == "READ_REQUEST_COUNT":
			message = read_requet_count_ride()
			return message, 200

		if request.args.get('COMMAND') == "READ_RIDE_COUNT":
			message = read_ride_count()
			return message, 200
	# rides-end


# api 9
@app.route('/api/v1/db/write', methods=['POST'])
def db_write():
	req = json.loads(request.data)
	# rides-start
	if (req['ORIGIN'] == 'RIDE'):
		if (req['COMMAND'] == 'INSERT'):
			collection_name = req['COLLECTION']
			data = {}
			fields = req['FIELDS']
			for field in range(len(fields)):
				data[fields[field]] = req["VALUES"][field]
			message = create_ride(collection_name, data)
			if (message):
				return make_response("",201)

		if(req['COMMAND'] == 'DELETE'):
			collection_name = req['COLLECTION']
			data = {
				req['FIELD']:req['VALUE']
			}
			message = delete_ride(collection_name, data)
			if (message):
				return make_response("",201)

		if(req['COMMAND'] == 'Update_Ride'):
			username = req['username']
			id = req['id']
			message = update_ride(username, id)
			collection = dbState('Rides')
			message = collection.find_one({"_id":id})
			if(message):
				return make_response('',200)
			else:
				return make_response('',400)
				

		if (req['COMMAND'] == "DELETE_ALL"):
			message = delete_all_ride()
			if (message):
				return make_response('',200)

		if (req['COMMAND'] == "DELETE_REQUEST_COUNT"):
			message = delete_request_count_ride()
			if (message):
				return make_response('', 200)

		if (req['COMMAND'] == "ADD_REQUEST_COUNT"):
			message = add_request_count_ride()
			if (message):
				return make_response('', 200)

		if (req['COMMAND'] == "ADD_RIDE_COUNT"):
			message = add_ride_count()
			if (message):
				return make_response('', 200)
	# rides-end


if __name__ == '__main__':
	client = pymongo.MongoClient('mongodb://rides_mongodb:27017/')
	dbnames = client.list_database_names()
	if "RideShare" in dbnames:
		db = client["RideShare"]
		db["Rides"].drop()
		db["RideCount"].drop()
		db["Area"].drop()
	Add_area()
	collection = dbState("RideCount")
	collection.insert_one({"_id" : "ride", "count" : 0})
	collection.insert_one({"_id" : "request", "count" : 0})
	app.run(host='0.0.0.0', debug = False)