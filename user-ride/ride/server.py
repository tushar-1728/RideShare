from flask import Flask, request, jsonify, make_response, redirect
import requests
from datetime import datetime
import json
import csv
import sys

app = Flask(__name__)

# api 3
@app.route("/api/v1/rides",methods=["POST"])
def addRide():
	requests.post("http://34.238.33.155:80/api/v1/db/write", data=json.dumps({"ORIGIN":"RIDE", "COMMAND":"ADD_REQUEST_COUNT"}))
	req = request.get_json()
	if req == "":
		return make_response('',400)
	username = req['created_by']
	source = req['source']
	destination = req['destination']
	timestamp = req['timestamp']
	if username == '' or username == None or source == '' or source == None or destination == '' or destination == None or timestamp == '' or timestamp == None:
		return make_response('Any of the 4 given fields is empty.',400)
	
	try:
		datetime.strptime(timestamp,"%d-%m-%Y:%S-%M-%H")

		if datetime.strptime(datetime.now().strftime("%d-%m-%Y:%S-%M-%H"),"%d-%m-%Y:%S-%M-%H") >= datetime.strptime(timestamp,"%d-%m-%Y:%S-%M-%H"):
			return make_response('Timestamp is invalid.',400)

		user_list = requests.get("http://54.227.171.39:80/api/v1/users", headers={'Origin': '54.227.171.39'}).json()
		if(username not in user_list):
			return make_response('User does not exist.',400)

		if requests.get("http://34.238.33.155:80/api/v1/db/read", params={"ORIGIN":"RIDE", "COMMAND":"EXISTS", "FIELD":"Area No", "VALUE":source, "COLLECTION":"Area"}).json()["count"] == 0 or requests.get("http://34.238.33.155:80/api/v1/db/read", params={"ORIGIN":"RIDE", "COMMAND":"EXISTS", "FIELD":"Area No", "VALUE":destination, "COLLECTION":"Area"}).json()["count"] == 0:
			return make_response('Either source or destination is invalid.',400)

		requests.post("http://34.238.33.155:80/api/v1/db/write", data=json.dumps({"ORIGIN":"RIDE", "COMMAND":"INSERT", "FIELDS":["created_by","users","timestamp","source","destination"], "VALUES":[username,[username],timestamp,source,destination], "COLLECTION":"Rides"}))

		requests.post("http://34.238.33.155:80/api/v1/db/write", data=json.dumps({"ORIGIN":"RIDE", "COMMAND":"ADD_RIDE_COUNT"}))
		return make_response('',201)
	except:
		return make_response('Unknown error',400)


# api 4
@app.route('/api/v1/rides', methods = ['GET'])
def list_rides():
	requests.post("http://34.238.33.155:80/api/v1/db/write", data=json.dumps({"ORIGIN":"RIDE", "COMMAND":"ADD_REQUEST_COUNT"}))
	source = request.args.get('source')
	destination = request.args.get('destination')
	if source == '' or source == None or destination == None or destination == '':
		return make_response('Either source or destination is empty.',400)

	if requests.get("http://34.238.33.155:80/api/v1/db/read", params={"ORIGIN":"RIDE", "COMMAND":"EXISTS", "FIELD":"Area No", "VALUE":source, "COLLECTION":"Area"}).json()["count"] == 0 or requests.get("http://34.238.33.155:80/api/v1/db/read", params={"ORIGIN":"RIDE", "COMMAND":"EXISTS", "FIELD":"Area No", "VALUE":destination, "COLLECTION":"Area"}).json()["count"] == 0:
			return make_response('Either source or destination is invalid.',400)

	res = requests.get('http://34.238.33.155:80/api/v1/db/read',params={"ORIGIN":"RIDE", 'COMMAND':'Upcoming','source':source,'destination':destination})
	if res.status_code == 204:
		return make_response('',204)
	return make_response(jsonify(res.json()['upcoming']),200)


# api 5
@app.route('/api/v1/rides/<rideid>', methods=['GET'])
def details_ride(rideid):
	requests.post("http://34.238.33.155:80/api/v1/db/write", data=json.dumps({"ORIGIN":"RIDE", "COMMAND":"ADD_REQUEST_COUNT"}))
	message = {}
	if rideid == '' or rideid == None:
		return make_response('Rideid is empty.',400)

	if requests.get("http://34.238.33.155:80/api/v1/db/read", params={"ORIGIN":"RIDE", "COMMAND":"EXISTS", "FIELD":"_id", "VALUE":rideid, "COLLECTION":"Rides"}).json()["count"] == 0:
		return make_response('Rideid is invalid.',400)

	message = requests.get('http://34.238.33.155:80/api/v1/db/read',params={"ORIGIN":"RIDE", 'COMMAND':'Ride_Details','id':rideid}).json()
	message = dict(message)
	message["RideID"] = message["_id"]
	del message["_id"]
	return make_response(jsonify(message),200)


# api 6
@app.route('/api/v1/rides/<rideid>', methods=['POST'])
def Join_ride(rideid):
	requests.post("http://34.238.33.155:80/api/v1/db/write", data=json.dumps({"ORIGIN":"RIDE", "COMMAND":"ADD_REQUEST_COUNT"}))
	req = request.get_json()
	if req == None:
		return make_response('',400)
	if 'username' not in req:
		return make_response(jsonify('Request does not consist of username.'),400)

	username = req["username"]

	if username == '' or username == None or rideid == '' or rideid == None:
		return make_response('username name is empty.',400)

	if requests.get("http://34.238.33.155:80/api/v1/db/read", params={"ORIGIN":"RIDE", "COMMAND":"EXISTS", "FIELD":"_id", "VALUE":rideid, "COLLECTION":"Rides"}).json()["count"] == 0:
		return make_response('RideID does not exist.',400)

	user_list = requests.get("http://54.227.171.39:80/api/v1/users",headers={'Origin': '54.227.171.39'}).json()
	if(username not in user_list):
		print(user_list)
		return make_response('username does not exist.',400)

	message = requests.get('http://34.238.33.155:80/api/v1/db/read',params={"ORIGIN":"RIDE", 'COMMAND':'Ride_Details','id':int(rideid)}).json()
	message = dict(message)
	if(username in message["users"]):
		return make_response("creator of a ride can not join his own ride.", 400)
	if datetime.strptime(datetime.now().strftime("%d-%m-%Y:%S-%M-%H"),"%d-%m-%Y:%S-%M-%H") > datetime.strptime(message["timestamp"],"%d-%m-%Y:%S-%M-%H"):
		return make_response("check time-stamp", 400)

	requests.post("http://34.238.33.155:80/api/v1/db/write", data=json.dumps({"ORIGIN":"RIDE", "COMMAND":"Update_Ride", "id":int(rideid), "username":username}))

	return make_response('',200)


# api 7
@app.route('/api/v1/rides/<rideid>', methods=['DELETE'])
def deleteride(rideid):
	requests.post("http://34.238.33.155:80/api/v1/db/write", data=json.dumps({"ORIGIN":"RIDE", "COMMAND":"ADD_REQUEST_COUNT"}))
	if rideid == '' or rideid == None:
		return make_response('RideID is empty.',400)

	if requests.get("http://34.238.33.155:80/api/v1/db/read", params={"ORIGIN":"RIDE", "COMMAND":"EXISTS", "FIELD":"_id", "VALUE":rideid, "COLLECTION":"Rides"}).json()["count"] == 0:
		return make_response('There are no rides with given ride-id.',400)

	requests.post("http://34.238.33.155:80/api/v1/db/write", data=json.dumps({"ORIGIN":"RIDE", "COMMAND":"DELETE", "FIELD":"_id", "VALUE":int(rideid), "COLLECTION":"Rides"}))
	return make_response('',200)


#api 10
@app.route('/api/v1/db/clear', methods=['POST'])
def delete_all():
	requests.post("http://34.238.33.155:80/api/v1/db/write", data=json.dumps({"ORIGIN":"RIDE", "COMMAND":"DELETE_ALL"}))
	return make_response('', 200)

#api 11
@app.route('/api/v1/_count', methods=['GET'])
def count_requests():
	msg = requests.get("http://34.238.33.155:80/api/v1/db/read", params={"ORIGIN":"RIDE", "COMMAND":"READ_REQUEST_COUNT"})
	if(msg.status_code == 200):
		return make_response(jsonify(msg.json()['count']), 200)

#api 12
@app.route('/api/v1/_count', methods=['DELETE'])
def reset_request_count():
	requests.post("http://34.238.33.155:80/api/v1/db/write", data=json.dumps({"ORIGIN":"RIDE", "COMMAND":"RESET_REQUEST_COUNT"}))
	return make_response('', 200)

#api 13
@app.route('/api/v1/rides/count', methods=['GET'])
def count_rides_created():
	requests.post("http://34.238.33.155:80/api/v1/db/write", data=json.dumps({"ORIGIN":"RIDE", "COMMAND":"ADD_REQUEST_COUNT"}))
	msg = requests.get("http://34.238.33.155:80/api/v1/db/read", params={"ORIGIN":"RIDE", "COMMAND":"READ_RIDE_COUNT"})
	if(msg.status_code == 200):
		return make_response(jsonify(msg.json()['count']), 200)


@app.route('/api/v1/rides/health_check', methods=['GET'])
def health_check():
	return make_response('', 200)


if __name__ == '__main__':
	app.run(host='0.0.0.0', debug = False)