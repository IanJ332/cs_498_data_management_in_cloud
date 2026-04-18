from flask import Flask, jsonify, request
from neo4j import GraphDatabase
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, round
app = Flask(__name__)

# Spark Session initialization
spark = SparkSession.builder \
    .appName("TaxiAPI") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()

base_df = spark.read.csv("taxi_trips_clean.csv", header=True, inferSchema=True).cache() 

# Connection details
uri = "bolt://136.116.212.210:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "jiangjs030302"))

# 1. Graph Summary
@app.route('/graph-summary', methods=['GET'])
def get_graph_summary():
    with driver.session() as session:
        # Get counts for Drivers, Companies, Areas, and Trips
        d_count = session.run("MATCH (d:Driver) RETURN count(d) as c").single()['c']
        c_count = session.run("MATCH (c:Company) RETURN count(c) as c").single()['c']
        a_count = session.run("MATCH (a:Area) RETURN count(a) as c").single()['c']
        t_count = session.run("MATCH ()-[r:TRIP]->() RETURN count(r) as c").single()['c']

        return jsonify({
            "driver_count": d_count,
            "company_count": c_count,
            "area_count": a_count,
            "trip_count": t_count
        })

# 2. Top Companies
@app.route('/top-companies', methods=['GET'])
def get_top_companies():
    n = int(request.args.get('n', 5))
    with driver.session() as session:
        # Rank companies by total number of trips
        query = """
        MATCH (c:Company)<-[:WORKS_FOR]-(d:Driver)-[:TRIP]->()
        RETURN c.name as name, count(*) as trip_count
        ORDER BY trip_count DESC LIMIT $n
        """
        results = session.run(query, n=n)
        companies = [{"name": r["name"], "trip_count": r["trip_count"]} for r in results]
        return jsonify({"companies": companies})

# 3. High-Fare Trips
@app.route('/high-fare-trips', methods=['GET'])
def get_high_fare_trips():
    area_id = int(request.args.get('area_id'))
    min_fare = float(request.args.get('min_fare'))
    with driver.session() as session:
        # Get trips to a specific area with fare above min_fare
        query = """
        MATCH (d:Driver)-[t:TRIP]->(a:Area {area_id: $area_id})
        WHERE t.fare > $min_fare
        RETURN t.trip_id as trip_id, t.fare as fare, d.driver_id as driver_id
        ORDER BY fare DESC
        """
        results = session.run(query, area_id=area_id, min_fare=min_fare)
        trips = [dict(r) for r in results]
        return jsonify({"trips": trips})
# 4. Co-Area Drivers
@app.route('/co-area-drivers', methods=['GET'])
def get_co_area_drivers():
    driver_id = request.args.get('driver_id')
    with driver.session() as session:
        # Find drivers who traveled to the same areas as the given driver
        query = """
        MATCH (d1:Driver {driver_id: $driver_id})-[:TRIP]->(a:Area)<-[:TRIP]-(d2:Driver)
        WHERE d1 <> d2
        RETURN d2.driver_id as driver_id, count(DISTINCT a) as shared_areas
        ORDER BY shared_areas DESC
        """
        results = session.run(query, driver_id=driver_id)
        co_drivers = [dict(r) for r in results]
        return jsonify({"co_area_drivers": co_drivers})

# 5. Average Fare by Company
@app.route('/avg-fare-by-company', methods=['GET'])
def get_avg_fare():
    with driver.session() as session:
        # Calculate average fare per company
        query = """
        MATCH (c:Company)<-[:WORKS_FOR]-(d:Driver)-[t:TRIP]->()
        RETURN c.name as name, round(avg(t.fare), 2) as avg_fare
        ORDER BY avg_fare DESC
        """
        results = session.run(query)
        avg_fares = [dict(r) for r in results]
        return jsonify({"companies": avg_fares})
@app.route('/area-stats', methods=['GET'])
def get_area_stats():
    area_id = int(request.args.get('area_id'))

    # Using the pre-loaded base_df
    stats = base_df.filter(col("dropoff_area") == area_id) \
              .groupBy() \
              .agg(
                  count("*").alias("trip_count"),
                  round(avg("fare"), 2).alias("avg_fare"),
                  round(avg("trip_seconds"), 0).alias("avg_trip_seconds")
              ).collect()

    if not stats or stats[0]["trip_count"] == 0:
        return jsonify({"error": "area not found"}), 404

    res = stats[0]
    return jsonify({
        "area_id": area_id,
        "trip_count": res["trip_count"],
        "avg_fare": res["avg_fare"],
        "avg_trip_seconds": int(res["avg_trip_seconds"])
    })

@app.route('/top-pickup-areas', methods=['GET'])
def get_top_pickup():
    n = int(request.args.get('n', 5))

    # Directly use pre-loaded df
    top_areas = base_df.groupBy("pickup_area") \
                  .agg(count("*").alias("trip_count")) \
                  .orderBy(col("trip_count").desc()) \
                  .limit(n).collect()

    return jsonify({"areas": [row.asDict() for row in top_areas]})

@app.route('/company-compare', methods=['GET'])
def compare_companies():
    c1 = request.args.get('company1')
    c2 = request.args.get('company2')

    # Add column and register view only if needed,
    # or better yet, prepare it during initialization
    df_with_fare = base_df.withColumn("fare_per_minute", col("fare") / (col("trip_seconds") / 60.0))
    df_with_fare.createOrReplaceTempView("trips")

    # Use parameters to prevent logic issues with quotes in names
    query = """
    SELECT
        company,
        COUNT(*) AS trip_count,
        ROUND(AVG(fare), 2) AS avg_fare,
        ROUND(AVG(fare_per_minute), 2) AS avg_fare_per_minute,
        ROUND(AVG(trip_seconds), 0) AS avg_trip_seconds
    FROM trips
    WHERE company = ? OR company = ?
    GROUP BY company
    """
    # Execute query with parameters
    results = spark.sql(query, args=[c1, c2]).collect()
    if len(results) < 2:
        return jsonify({"error": "one or more companies not found"}), 404 
    return jsonify({"comparison": [row.asDict() for row in results]})
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)