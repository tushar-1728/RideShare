import csv
import pymongo

def Add_area():
    myclient = pymongo.MongoClient()
    db = myclient["RideShare"]
    collection = db["Area"]
    with open(r"AreaNameEnum.csv","r") as f:
        readCSV = list(csv.DictReader(f))
        for i in range(0, len(readCSV)):
            # readCSV[i] = dict(readCSV[i])
            readCSV[i]['_id'] = int(readCSV[i]['_id'])
            readCSV[i]['Area No'] = int(readCSV[i]['Area No'])

        collection.insert_many(readCSV)

client = pymongo.MongoClient()
dbnames = client.list_database_names()
if "RideShare" in dbnames:
    client.drop_database("RideShare")
Add_area()