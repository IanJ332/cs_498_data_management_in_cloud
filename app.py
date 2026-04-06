from flask import Flask, request, jsonify
from pymongo import MongoClient, WriteConcern, ReadPreference
import json

app = Flask(__name__)

MONGO_URI = "mongodb+srv://lab6user:030302@cluster0.s42bhpa.mongodb.net/?appName=Cluster0"
client = MongoClient(MONGO_URI)
db = client['ev_db']
collection = db['vehicles']

@app.route('/insert-fast', methods=['POST'])
def insert_fast():
    record = request.json
    coll_fast = collection.with_options(write_concern=WriteConcern(w=1))
    result = coll_fast.insert_one(record)
    return str(result.inserted_id), 201

@app.route('/insert-safe', methods=['POST'])
def insert_safe():
    record = request.json
    coll_safe = collection.with_options(write_concern=WriteConcern(w='majority'))
    result = coll_safe.insert_one(record)
    return str(result.inserted_id), 201

@app.route('/count-tesla-primary', methods=['GET'])
def count_tesla():
    coll_primary = collection.with_options(read_preference=ReadPreference.PRIMARY)
    count = coll_primary.count_documents({"Make": "TESLA"})
    return jsonify({"count": count})

@app.route('/count-bmw-secondary', methods=['GET'])
def count_bmw():
    coll_secondary = collection.with_options(read_preference=ReadPreference.SECONDARY_PREFERRED)
    count = coll_secondary.count_documents({"Make": "BMW"})
    return jsonify({"count": count})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)