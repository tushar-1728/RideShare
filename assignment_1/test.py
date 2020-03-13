import pymongo

myclient = pymongo.MongoClient()
db = myclient["RideShare"]
collection = db["Rides"]

print(collection.count_documents({"created_by": "Sagar", "timestamp": "01-09-2020:05-05-05"}))
