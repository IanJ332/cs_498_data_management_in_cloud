from fastapi import FastAPI, Body
from pymongo import MongoClient, WriteConcern, ReadPreference
import certifi

app = FastAPI()

MONGO_URI = "mongodb+srv://lab6user:PASSWORDS@cluster0.s42bhpa.mongodb.net/?appName=Cluster0"
client = MongoClient(MONGO_URI, tlsCAFile=certifi.where())
db = client['ev_db']
collection = db['vehicles']

@app.post("/insert-fast")
def insert_fast(record: dict = Body(...)):
    coll_fast = collection.with_options(write_concern=WriteConcern(w=1))
    result = coll_fast.insert_one(record)
    return {"id": str(result.inserted_id)}

@app.post("/insert-safe")
def insert_safe(record: dict = Body(...)):
    coll_safe = collection.with_options(write_concern=WriteConcern(w='majority'))
    result = coll_safe.insert_one(record)
    return {"id": str(result.inserted_id)}

@app.get("/count-tesla-primary")
def count_tesla():
    coll_primary = collection.with_options(read_preference=ReadPreference.PRIMARY)
    count = coll_primary.count_documents({"Make": "TESLA"})
    return {"count": count}

@app.get("/count-bmw-secondary")
def count_bmw():
    coll_secondary = collection.with_options(read_preference=ReadPreference.SECONDARY_PREFERRED)
    count = coll_secondary.count_documents({"Make": "BMW"})
    return {"count": count}
