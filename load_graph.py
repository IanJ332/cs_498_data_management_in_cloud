import pandas as pd
from neo4j import GraphDatabase

# Using your verified IP and password
uri = "bolt://136.116.212.210:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "jiangjs030302"))

def ingest_data(tx, row):
    # Use MERGE on nodes to avoid duplicates as required [cite: 97]
    # Relationships: WORKS_FOR (Driver->Company) and TRIP (Driver->Area) [cite: 91-94]
    query = """
    MERGE (d:Driver {driver_id: $driver_id})
    MERGE (c:Company {name: $company_name})
    MERGE (a:Area {area_id: $area_id})
    MERGE (d)-[:WORKS_FOR]->(c)
    CREATE (d)-[:TRIP {
        trip_id: $trip_id,
        fare: $fare,
        trip_seconds: $trip_seconds
    }]->(a)
    """
    tx.run(query,
           driver_id=str(row['driver_id']),
           company_name=str(row['company']),
           area_id=int(row['dropoff_area']),
           trip_id=str(row['trip_id']),
           fare=float(row['fare']),
           trip_seconds=int(row['trip_seconds']))

def main():
    # Input must be the cleaned 10k rows [cite: 79]
    df = pd.read_csv("taxi_trips_clean.csv")
    with driver.session() as session:
        print("Ingesting 10,000 trips into Neo4j...")
        for _, row in df.iterrows():
            session.execute_write(ingest_data, row)
        print("Graph loading complete.")
    driver.close()

if __name__ == "__main__":
    main()
