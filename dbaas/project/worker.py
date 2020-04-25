from datetime import datetime
import json
import csv
import sys
import pymongo
import pika




def dbState(client_name, collection_name):
	try:
		myclient = pymongo.MongoClient('mongodb://' + client_name + ':27017/')
	except:
		print("dbstate not working")
		return False
	db = myclient["RideShare"]
	collection = db[collection_name]
	return collection


def Add_area(client_name):
	myclient = pymongo.MongoClient('mongodb://' + client_name + ':27017/')
	db = myclient["RideShare"]
	collection = db["Area"]
	with open(r"AreaNameEnum.csv","r") as f:
		readCSV = list(csv.DictReader(f))
		for i in range(0, len(readCSV)):
			readCSV[i]['_id'] = int(readCSV[i]['_id'])
			readCSV[i]['Area No'] = int(readCSV[i]['Area No'])
		collection.insert_many(readCSV)


def get_upcoming_rides(args):
	source, destination = args.split(",")
	message = []
	collection = dbState("slave-mongodb", 'Rides')
	for rides in collection.find({"source":source,"destination":destination},{"_id":1,"created_by":1,"timestamp":1}):
		if datetime.strptime(datetime.now().strftime("%d-%m-%Y:%S-%M-%H"),"%d-%m-%Y:%S-%M-%H") < datetime.strptime(rides["timestamp"],"%d-%m-%Y:%S-%M-%H"):
			rides = {
				"rideId": rides["_id"],
				"username": rides["created_by"],
				"timestamp":rides["timestamp"]
			}
			message.append(rides)
	return json.dumps({"message":message}).encode()


def entry_exists(args):
	collection_name, field, val = args.split(",")
	collection = dbState("slave-mongodb", collection_name)
	try:
		val = int(val)
	except:
		val = str(val)
	data = {field: val}
	return json.dumps({"count":collection.count_documents(data)}).encode()


def get_ride_details(args):
	id = args
	collection = dbState("slave-mongodb", 'Rides')
	return json.dumps(collection.find_one({'_id':int(id)})).encode()

def read_request_count_ride():
	collection = dbState("slave-mongodb", "RideCount")
	count = collection.find_one({"_id" : "request"})["count"]
	return json.dumps({"count":count}).encode()


def read_ride_count():
	collection = dbState("slave-mongodb", "RideCount")
	count = collection.find_one({"_id" : "ride"})["count"]
	return json.dumps({"count":count}).encode()


def create_entry(body):
	collection = dbState("master-mongodb", body["collection"])
	data = body["data"]
	if collection.count_documents({"_id":"Last_Id"}) == 0:
		collection.insert_one({"_id":"Last_Id","Last_Id":0})
	Last_Id = collection.find_one({"_id":"Last_Id"})["Last_Id"] + 1
	data["_id"] = Last_Id
	collection.insert_one(data)
	collection.update_one({"_id":"Last_Id" }, {"$set" : {"Last_Id":Last_Id}})
	return "1".encode()


def delete_entry(body):
	collection = dbState("master-mongodb", body["collection"])
	collection.delete_one(body["data"])
	return "1".encode()


def update_ride(body):
	collection = dbState("master-mongodb", 'Rides')
	message = collection.find_one({"_id":body["id"]})
	if body["username"] not in message['users']:
		if datetime.strptime(datetime.now().strftime("%d-%m-%Y:%S-%M-%H"),"%d-%m-%Y:%S-%M-%H") < datetime.strptime(message["timestamp"],"%d-%m-%Y:%S-%M-%H"):
			message['users'].append(body["username"])
		else:
			return "0".encode()
	else:
		return "0".encode()
	collection.update_one({"_id":body["id"]}, {"$set" : {"users":message["users"]}})
	return "1".encode()


def reset_request_count_ride():
	collection = dbState("master-mongodb", "RideCount")
	collection.update_one({'_id': "request"}, {'$set': {'count': 0}})
	return "1".encode()


def add_request_count_ride():
	collection = dbState("master-mongodb", "RideCount")
	collection.update_one({'_id': "request"}, {'$inc': {'count': 1}})
	return "1".encode()


def add_ride_count():
	collection = dbState("master-mongodb", "RideCount")
	collection.update_one({'_id': "ride"}, {'$inc': {'count': 1}})
	return "1".encode()


def read_all_users():
	collection = dbState("slave-mongodb", 'Users')
	if(collection.count_documents({"_id": {'$gte': 1}})):
		users = collection.find()
		user_names = []
		count = 0
		for i in users:
			if(count > 0):
				user_names.append(i['username'])
			count += 1
		return json.dumps({"readall":user_names}).encode()
	else:
		return "0".encode()

def read_request_count_user():
	collection = dbState("slave-mongodb", "UserCount")
	count = collection.find_one({"_id" : 0})["count"]
	return json.dumps({"count":count}).encode()


def delete_all(body):
	collection = dbState("master-mongodb", body["collection"])
	collection.remove({})
	return "1".encode()


def reset_request_count_user():
	collection = dbState("master-mongodb", "UserCount")
	collection.update_one({'_id': 0}, {'$set': {'count': 0}})
	return "1".encode()


def add_request_count_user():
	collection = dbState("master-mongodb", "UserCount")
	collection.update_one({'_id': 0}, {'$inc': {'count': 1}})
	return "1".encode()


def db_init(client_name):
	client = pymongo.MongoClient('mongodb://' + client_name + ':27017/')
	dbnames = client.list_database_names()
	if "RideShare" in dbnames:
		db = client["RideShare"]
		db["Rides"].drop()
		db["RideCount"].drop()
		db["Area"].drop()
		db["Users"].drop()
		db["UserCount"].drop()
	Add_area(client_name)
	collection = dbState(client_name, "RideCount")
	collection.insert_one({"_id" : "ride", "count" : 0})
	collection.insert_one({"_id" : "request", "count" : 0})
	collection = dbState(client_name, "UserCount")
	collection.insert_one({"_id" : 0, "count" : 0})


def on_read_request(ch, method, props, body):
	body = body.decode()
	func_name, args = body.split(":")
	print("Read request received for " + func_name)

	if(func_name == "get_upcoming_rides"):
		response = get_upcoming_rides(args)
	if(func_name == "entry_exists"):
		response = entry_exists(args)
	if(func_name == "get_ride_details"):
		response = get_ride_details(args)
	if(func_name == "read_request_count_ride"):
		response = read_request_count_ride()
	if(func_name == "read_ride_count"):
		response = read_ride_count()
	if(func_name == "read_all_users"):
		response = read_all_users()
	if(func_name == "read_request_count_user"):
		response = read_request_count_user()

	ch.basic_publish(
		exchange='',
		routing_key=props.reply_to,
		properties=pika.BasicProperties(correlation_id = props.correlation_id),
		body=response
	)
	ch.basic_ack(delivery_tag=method.delivery_tag)


def on_write_request(ch, method, props, body):
	body = json.loads(body.decode())
	func_name = body["func"]
	print("Write request received for " + func_name)

	if(func_name == "create_entry"):
		response = create_entry(body)
	if(func_name == "delete_entry"):
		response = delete_entry(body)
	if(func_name == "update_ride"):
		response = update_ride(body)
	if(func_name == "delete_all"):
		response = delete_all(body)
	if(func_name == "reset_request_count_ride"):
		response = reset_request_count_ride()
	if(func_name == "add_request_count_ride"):
		response = add_request_count_ride()
	if(func_name == "add_ride_count"):
		response = add_ride_count()
	if(func_name == "reset_request_count_user"):
		response = reset_request_count_user()
	if(func_name == "add_request_count_user"):
		response = add_request_count_user()

	ch.basic_publish(
		exchange='',
		routing_key=props.reply_to,
		properties=pika.BasicProperties(correlation_id = props.correlation_id),
		body=response
	)
	ch.basic_ack(delivery_tag=method.delivery_tag)


if __name__ == '__main__':
	connection = pika.BlockingConnection(pika.ConnectionParameters(host='rmq'))
	designation = int(sys.argv[1])
	print("Ready for receiving requests.")
	if(designation == 1):
		print("master mode")
		client_name = "master-mongodb"
		db_init(client_name)
		channel_write = connection.channel()
		channel_write.queue_declare(queue="writeQ")
		channel_write.basic_qos(prefetch_count=1)
		channel_write.basic_consume(queue="writeQ", on_message_callback=on_write_request)
		channel_write.start_consuming()
	elif(designation == 0):
		print("slave mode")
		client_name = "slave-mongodb"
		db_init(client_name)
		channel_read = connection.channel()
		channel_read.queue_declare(queue='readQ')
		channel_read.basic_qos(prefetch_count=1)
		channel_read.basic_consume(queue='readQ', on_message_callback=on_read_request)
		channel_read.start_consuming()
