from flask import Flask, request, jsonify, make_response, redirect
import pymongo
import requests
from datetime import datetime
import json
import csv
import sys

app = Flask(__name__)

def dbState(collection):
	try:
		myclient = pymongo.MongoClient('mongodb://mongodb:27017/')
	except:
		return False,False
	db = myclient["RideShare"]
	collection = db[collection]
	return db, collection

def is_sha1(maybe_sha):
	if len(maybe_sha) != 40:
		return False
	try:
		sha_int = int(maybe_sha, 16)
	except:
		return False
	return True

def add_request_count():
	db, collection = dbState("UserCount")
	collection.update_one({'_id': 0}, {'$inc': {'count': 1}})

# api 1
@app.route('/api/v1/users', methods=['PUT'])
def adduser():
	add_request_count()
	req = request.get_json()
	if req == None:
		return make_response('',400)
	if 'username' not in req or 'password' not in req:
		return make_response('',400)
	username = req["username"]
	password = req["password"]
	if username == '' or username == None or password == None or password == '':
		return make_response('',400)

	if (not is_sha1(password)):
		return make_response('',400)

	if (requests.get("http://localhost:5000/api/v1/db/read", params={"COMMAND":"EXISTS", "FIELD":"username", "VALUE":username, "DB":"Users"})).json()["count"] != 0:
		return make_response('',400)

	requests.post("http://localhost:5000/api/v1/db/write", data=json.dumps({"COMMAND":"INSERT", "FIELDS":["username","password"], "VALUES":[username,password], "DB":"Users"}))

	return make_response('',201)


# api 2
@app.route('/api/v1/users/<username>', methods=['DELETE'])
def deleteuser(username):
	add_request_count()
	if username == '' or username == None:
		return make_response('',400)

	if (requests.get("http://localhost:5000/api/v1/db/read", params={"COMMAND":"EXISTS", "FIELD":"username", "VALUE":username, "DB":"Users"})).json()["count"] == 0:
		return make_response('',400)

	requests.post("http://localhost:5000/api/v1/db/write", data=json.dumps({"COMMAND":"DELETE", "FIELD":"username", "VALUE":username, "DB":"Users"}))

	return make_response('',200)

#api 8
@app.route('/api/v1/db/read', methods=['GET'])
def db_read():
	if request.args.get('COMMAND') == "Upcoming":
		db, collection = dbState('Rides')
		message = []
		source = request.args.get('source')
		destination = request.args.get('destination')
		for rides in collection.find({"source":source,"destination":destination},{"_id":1,"created_by":1,"timestamp":1}):
			if datetime.strptime(datetime.now().strftime("%d-%m-%Y:%S-%M-%H"),"%d-%m-%Y:%S-%M-%H") < datetime.strptime(rides["timestamp"],"%d-%m-%Y:%S-%M-%H"):
				rides = {
					"rideId": rides["_id"],
					"username": rides["created_by"],
					"timestamp":rides["timestamp"]
				}
				message.append(rides)
		if message == []:
			return make_response('',204)
		return json.dumps({'upcoming':message}),200

	if request.args.get('COMMAND') == "EXISTS":
		db, collection = dbState(request.args.get("DB"))
		val = request.args.get('VALUE')
		try:
			val = int(val)
		except:
			val = str(val)
		field = str(request.args.get("FIELD"))
		data = {field: val}
		return json.dumps({"count":collection.count_documents(data)}), 200

	if request.args.get('COMMAND') == "Ride_Details":
		db, collection = dbState('Rides')
		return json.dumps(collection.find_one({'_id':int(request.args.get('id'))})), 200

	if request.args.get('COMMAND') == "READ_ALL":
		db, collection = dbState('Users')
		if(collection.count_documents({"_id": {'$gte': 1}})):
			users = collection.find()
			user_names = []
			count = 0
			for i in users:
				if(count > 0):
					user_names.append(i['username'])
				count += 1
			return json.dumps({"readall":user_names}), 200
		else:
			return make_response('',204)


# api 9
@app.route('/api/v1/db/write', methods=['POST'])
def db_write():
	req = json.loads(request.data)
	if (req['COMMAND'] == 'INSERT'):
		db, collection = dbState(req['DB'])
		data = {}
		fields = req['FIELDS']
		for field in range(len(fields)):
			data[fields[field]] = req["VALUES"][field]

		if collection.count_documents({"_id":"Last_Id"}) == 0:
			collection.insert_one({"_id":"Last_Id","Last_Id":0})

		Last_Id = collection.find_one({"_id":"Last_Id"})["Last_Id"] + 1
		data["_id"] = Last_Id
		collection.insert_one(data)
		collection.update_one({"_id":"Last_Id" }, {"$set" : {"Last_Id":Last_Id}})
		return make_response(jsonify({}),201)

	if(req['COMMAND'] == 'DELETE'):
		db, collection = dbState(req['DB'])
		data = {
			req['FIELD']:req['VALUE']
		}
		collection.delete_one(data)
		return make_response(jsonify({}),200)

	if req['COMMAND'] == "Update_Ride":
		db, collection = dbState('Rides')
		message = collection.find_one({"_id":req['id']})
		if req['username'] not in message['users']:
			if datetime.strptime(datetime.now().strftime("%d-%m-%Y:%S-%M-%H"),"%d-%m-%Y:%S-%M-%H") < datetime.strptime(message["timestamp"],"%d-%m-%Y:%S-%M-%H"):
				message['users'].append(req["username"])
			else:
				return make_response('',400)
		else:
			return make_response('',400)
		collection.update_one({"_id":req['id'] }, {"$set" : {"users":message["users"]}})
		return make_response('',200)

	if (req['COMMAND'] == "DELETE_ALL"):
		db, collection = dbState('Users')
		collection.remove({})
		return make_response('',200)

#api 10
@app.route('/api/v1/users', methods=['GET'])
def read_all():
	add_request_count()
	msg = requests.get("http://localhost:5000/api/v1/db/read", params={"COMMAND":"READ_ALL", "DB":"Users"})
	if(msg.status_code == 204):
		return make_response('',204)
	elif(msg.status_code == 200):
		return make_response(jsonify(msg.json()['readall']), 200)

#api 11
@app.route('/api/v1/db/clear', methods=['POST'])
def delete_all():
	# add_request_count()
	requests.post("http://localhost:5000/api/v1/db/write", data=json.dumps({"COMMAND":"DELETE_ALL"}))
	return make_response('', 200)

#api 12
@app.route('/api/v1/_count', methods=['GET'])
def count_requests():
	db, collection = dbState("UserCount")
	count = collection.find_one({"_id" : 0})["count"]
	return make_response(jsonify(count), 200)

#api 13
@app.route('/api/v1/_count', methods=['DELETE'])
def reset_request_count():
	db, collection = dbState("UserCount")
	collection.update_one({'_id': 0}, {'$set': {'count': 0}})
	return make_response('', 200)

@app.route('/api/v1/users/health_check', methods=['GET'])
def health_check():
	return make_response('', 200)

if __name__ == '__main__':
	client = pymongo.MongoClient('mongodb://mongodb:27017/')
	dbnames = client.list_database_names()
	if "RideShare" in dbnames:
		db = client["RideShare"]
		db["Users"].drop()
		db["UserCount"].drop()
	db, collection = dbState("UserCount")
	collection.insert_one({"_id" : 0, "count" : 0})
	app.run(host='localhost', port = '5000', debug = False)
