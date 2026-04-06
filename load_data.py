import pandas as pd
from pymongo import MongoClient

MONGO_URI = "mongodb+srv://lab6user:password@cluster0.s42bhpa.mongodb.net/?appName=Cluster0"
DB_NAME = "ev_db"
COLLECTION_NAME = "vehicles"

def main():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    print("Reading .CSV File...")
    df = pd.read_csv('ev_data.csv')
    
    data = df.to_dict('records')

    batch_size = 1000
    print(f"Current Bacth {batch_size} speed import {len(data)} data..")
    
    for i in range(0, len(data), batch_size):
        batch = data[i : i + batch_size]
        collection.insert_many(batch)
    
    print("Sucess！")

if __name__ == "__main__":
    main()
