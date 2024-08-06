from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, udf
from pyspark.sql.types import StringType, DoubleType, StructType, StructField
import os
import time
from cassandra.cluster import Cluster
import uuid

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'


def generate_uuid():
    return str(uuid.uuid4())


uuid_udf = udf(generate_uuid, StringType())


def writeToCassandra(batch_df, batch_id):
    cassandra_cluster = None
    cassandra_session = None
    try:
        cassandra_cluster = Cluster(['cassandra1'], port=9042)
        cassandra_session = cassandra_cluster.connect()
        cassandra_session.set_keyspace("all_data_view")

        cassandra_session.execute(
            """
            CREATE TABLE IF NOT EXISTS real_time_view (
                id uuid PRIMARY KEY,
                item TEXT,
                count INT,
                time_stamp TIMESTAMP
            )
            """
        )

        batch_df_with_timestamp_and_uuid = batch_df \
            .withColumn("time_stamp", current_timestamp()) \
            .withColumn("id", uuid_udf())

        batch_df_with_timestamp_and_uuid.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="real_time_view", keyspace="all_data_view") \
            .mode("append") \
            .save()
    except Exception as e:
        print(f"Error writing to Cassandra: {e}")
    finally:
        if cassandra_session:
            cassandra_session.shutdown()
        if cassandra_cluster:
            cassandra_cluster.shutdown()


def startStream():
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("KafkaStreamProcessing") \
        .config("spark.cassandra.connection.host", "cassandra1") \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.master", os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077")) \
        .getOrCreate()

    # Define the schema for parsing JSON data
    schema = StructType([
        StructField("item", StringType()),
        StructField("customer_id", StringType()),
        StructField("healthy_food", StringType()),
        StructField("price", DoubleType()),
        StructField("snack_automat_id", StringType()),
        StructField("timestamp", StringType())
    ])

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("subscribe", "snack_automat_message") \
        .option("startingOffsets", "latest") \
        .load()

    # Parse JSON data
    parsed_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # Group by item and count
    result_df = parsed_df.groupBy("item").count()

    # Write result to Cassandra continuously
    query = result_df.writeStream \
        .trigger(processingTime="2 seconds") \
        .outputMode("complete") \
        .foreachBatch(writeToCassandra) \
        .option("spark.cassandra.connection.host", "cassandra1") \
        .start()

    return query


if __name__ == "__main__":
    while True:
        query = startStream()
        try:
            query.awaitTermination()
        except Exception as e:
            print(f"Error: {e}")
            query.stop()
            query.awaitTermination()
            print("Restarting the stream...")
            time.sleep(5)
