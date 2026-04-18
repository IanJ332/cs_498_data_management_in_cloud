from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, round

def main():
    # Initialize a Spark session
    spark = SparkSession.builder \
        .appName("TaxiDataPreprocessing") \
        .getOrCreate()

    # Read taxi_trips_clean.csv with header and schema inference
    df = spark.read.csv("taxi_trips_clean.csv", header=True, inferSchema=True)

    # Add fare_per_minute column
    df = df.withColumn("fare_per_minute", col("fare") / (col("trip_seconds") / 60.0))

    # Register the DataFrame as a SQL view named 'trips'
    df.createOrReplaceTempView("trips")

    # Compute company-level summary using Spark SQL
    query = """
    SELECT 
        company, 
        COUNT(*) AS trip_count, 
        ROUND(AVG(fare), 2) AS avg_fare, 
        ROUND(AVG(fare_per_minute), 2) AS avg_fare_per_minute 
    FROM trips 
    GROUP BY company 
    ORDER BY trip_count DESC
    """
    summary_df = spark.sql(query)

    # Save the resulting DataFrame to processed_data/ in JSON format
    summary_df.write.mode("overwrite").json("processed_data/")
    
    print("Preprocessing complete. Results saved to processed_data/")
    spark.stop()

if __name__ == "__main__":
    main()
