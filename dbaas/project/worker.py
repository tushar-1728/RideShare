from datetime import datetime
import json
import csv
import sys
import pymongo

def dbState(collection):
	try:
		myclient = pymongo.MongoClient('mongodb://rides_mongodb:27017/')
	except:
		return False,False
	db = myclient["RideShare"]
	collection = db[collection]
	return collection

def is_sha1(maybe_sha):
	if len(maybe_sha) != 40:
		return False
	try:
		int(maybe_sha, 16)
	except:
		return False
	return True

def checkareacode(areacode):
	collection_area = dbState("Area")
	if (collection_area.count_documents({"Area No":str(areacode)}) != 0):
		return True
	return False

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