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
	db, collection = dbState("RideCount")
	collection.update_one({'_id': "request"}, {'$inc': {'count': 1}})

def add_ride_count():
	db, collection = dbState("RideCount")
	collection.update_one({'_id': "ride"}, {'$inc': {'count': 1}})

def sub_ride_count():
	db, collection = dbState("RideCount")
	count = collection.find_one({"_id": "ride"})["count"]
	if(count > 0):
		collection.update_one({'_id': "ride"}, {'$inc': {'count': -1}})

def checkareacode(areacode):
	db, collection_area = dbState("Area")
	if (collection_area.count_documents({"Area No":str(areacode)}) != 0):
		return True
	return False

# api 3
@app.route("/api/v1/rides",methods=["POST"])
def addRide():
	add_request_count()
	req = request.get_json()
	if req == "":
		return make_response('',400)
	username = req['created_by']
	source = req['source']
	destination = req['destination']
	timestamp = req['timestamp']
	if username == '' or username == None or source == '' or source == None or destination == '' or destination == None or timestamp == '' or timestamp == None:
		print(1)
		return make_response('',400)
	
	try:
		datetime.strptime(timestamp,"%d-%m-%Y:%S-%M-%H")

		if datetime.strptime(datetime.now().strftime("%d-%m-%Y:%S-%M-%H"),"%d-%m-%Y:%S-%M-%H") >= datetime.strptime(timestamp,"%d-%m-%Y:%S-%M-%H"):
			print(2)
			return make_response('',400)

		user_list = requests.get("http://34.239.83.226:80/api/v1/users", headers={'Origin': '54.208.115.23'}).json()
		if(username not in user_list):
			print(3)
			return make_response('',400)

		if requests.get("http://localhost:5000/api/v1/db/read", params={"COMMAND":"EXISTS", "FIELD":"Area No", "VALUE":source, "DB":"Area"}).json()["count"] == 0 or requests.get("http://localhost:5000/api/v1/db/read", params={"COMMAND":"EXISTS", "FIELD":"Area No", "VALUE":destination, "DB":"Area"}).json()["count"] == 0:
			print(4)
			return make_response('',400)

		requests.post("http://localhost:5000/api/v1/db/write", data=json.dumps({"COMMAND":"INSERT", "FIELDS":["created_by","users","timestamp","source","destination"], "VALUES":[username,[username],timestamp,source,destination], "DB":"Rides"}))

		add_ride_count()
		return make_response('',201)
	except:
		print(5)
		return make_response('',400)


# api 4
@app.route('/api/v1/rides', methods = ['GET'])
def list_rides():
	add_request_count()
	source = request.args.get('source')
	destination = request.args.get('destination')
	message = []
	if source == '' or source == None or destination == None or destination == '':
		return make_response('',400)

	if requests.get("http://localhost:5000/api/v1/db/read", params={"COMMAND":"EXISTS", "FIELD":"Area No", "VALUE":source, "DB":"Area"}).json()["count"] == 0 or requests.get("http://localhost:5000/api/v1/db/read", params={"COMMAND":"EXISTS", "FIELD":"Area No", "VALUE":destination, "DB":"Area"}).json()["count"] == 0:
			return make_response('',400)

	res = requests.get('http://localhost:5000/api/v1/db/read',params={'COMMAND':'Upcoming','source':source,'destination':destination})
	if res.status_code == 204:
		return make_response('',204)
	return make_response(jsonify(res.json()['upcoming']),200)


# api 5
@app.route('/api/v1/rides/<rideid>', methods=['GET'])
def details_ride(rideid):
	add_request_count()
	message = {}
	if rideid == '' or rideid == None:
		return make_response('',400)

	if requests.get("http://localhost:5000/api/v1/db/read", params={"COMMAND":"EXISTS", "FIELD":"_id", "VALUE":rideid, "DB":"Rides"}).json()["count"] == 0:
		return make_response('',400)

	message = requests.get('http://localhost:5000/api/v1/db/read',params={'COMMAND':'Ride_Details','id':rideid}).json()
	message = dict(message)
	message["RideID"] = message["_id"]
	del message["_id"]
	return make_response(jsonify(message),200)


# api 6
@app.route('/api/v1/rides/<rideid>', methods=['POST'])
def Join_ride(rideid):
	add_request_count()
	req = request.get_json()
	if req == None:
		return make_response('',400)
	if 'username' not in req:
		return make_response(jsonify(''),400)

	username = req["username"]

	if username == '' or username == None or rideid == '' or rideid == None:
		print(1)
		return make_response('',400)

	if requests.get("http://localhost:5000/api/v1/db/read", params={"COMMAND":"EXISTS", "FIELD":"_id", "VALUE":rideid, "DB":"Rides"}).json()["count"] == 0:
		print(2)
		return make_response('',400)

	user_list = requests.get("http://34.239.83.226:80/api/v1/users",headers={'Origin': '54.208.115.23'}).json()
	if(username not in user_list):
		print(3)
		print(user_list)
		return make_response('',400)

	msg = requests.post("http://localhost:5000/api/v1/db/write", data=json.dumps({"COMMAND":"Update_Ride", "id":int(rideid), "username":username}))
	if msg.status_code == 400:
		print(4)
		return make_response('',400)

	return make_response('',200)


# api 7
@app.route('/api/v1/rides/<rideid>', methods=['DELETE'])
def deleteride(rideid):
	add_request_count()
	if rideid == '' or rideid == None:
		return make_response('',400)

	if requests.get("http://localhost:5000/api/v1/db/read", params={"COMMAND":"EXISTS", "FIELD":"_id", "VALUE":rideid, "DB":"Rides"}).json()["count"] == 0:
		return make_response('',400)

	requests.post("http://localhost:5000/api/v1/db/write", data=json.dumps({"COMMAND":"DELETE", "FIELD":"_id", "VALUE":int(rideid), "DB":"Rides"}))
	# sub_ride_count()
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

	if(req['COMMAND'] == 'Update_Ride'):
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
		db, collection = dbState('Rides')
		collection.remove({})
		return make_response('',200)

#api 10
@app.route('/api/v1/db/clear', methods=['POST'])
def delete_all():
	requests.post("http://localhost:5000/api/v1/db/write", data=json.dumps({"COMMAND":"DELETE_ALL"}))
	return make_response('', 200)

#api 11
@app.route('/api/v1/_count', methods=['GET'])
def count_requests():
	db, collection = dbState("RideCount")
	count = collection.find_one({"_id" : "request"})["count"]
	return make_response(jsonify(count), 200)

#api 12
@app.route('/api/v1/_count', methods=['DELETE'])
def reset_request_count():
	db, collection = dbState("RideCount")
	collection.update_one({'_id': "request"}, {'$set': {'count': 0}})
	return make_response('', 200)

#api 13
@app.route('/api/v1/rides/count', methods=['GET'])
def count_rides_created():
	add_request_count()
	db, collection = dbState("RideCount")
	count = collection.find_one({"_id" : "ride"})["count"]
	return make_response(jsonify(count), 200)

@app.route('/api/v1/rides/health_check', methods=['GET'])
def health_check():
	return make_response('', 200)

def Add_area():
	myclient = pymongo.MongoClient('mongodb://mongodb:27017/')
	db = myclient["RideShare"]
	collection = db["Area"]
	with open(r"AreaNameEnum.csv","r") as f:
		readCSV = list(csv.DictReader(f))
		for i in range(0, len(readCSV)):
			readCSV[i]['_id'] = int(readCSV[i]['_id'])
			readCSV[i]['Area No'] = int(readCSV[i]['Area No'])
		collection.insert_many(readCSV)

if __name__ == '__main__':
	client = pymongo.MongoClient('mongodb://mongodb:27017/')
	dbnames = client.list_database_names()
	if "RideShare" in dbnames:
		db = client["RideShare"]
		db["Rides"].drop()
		db["RideCount"].drop()
		db["Area"].drop()
	Add_area()
	db, collection = dbState("RideCount")
	collection.insert_one({"_id" : "ride", "count" : 0})
	collection.insert_one({"_id" : "request", "count" : 0})
	app.run(host='0.0.0.0', debug = False)
