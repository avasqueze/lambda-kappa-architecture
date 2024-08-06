from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, udf
from pyspark.sql.types import StringType, DoubleType, StructType, StructField, IntegerType
import os
from kafka import KafkaConsumer
import json
from cassandra.cluster import Cluster
import uuid

# Define Spark configuration
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

def generate_uuid():
    return str(uuid.uuid4())

uuid_udf = udf(generate_uuid, StringType())

def resetCountsInCassandra():
    cassandra_cluster = None
    cassandra_session = None
    try:
        cassandra_cluster = Cluster(['cassandra1'], port=9042)
        cassandra_session = cassandra_cluster.connect()
        cassandra_session.set_keyspace("all_data_view")

        items = ['apple', 'banana', 'orange', 'blueberries', 'mars', 'twix', 'snickers', 'milkyway']

        for item in items:
            cassandra_session.execute(
                f"INSERT INTO real_time_view (id, item, count, time_stamp) VALUES (uuid(), '{item}', 0, toTimestamp(now()))")
    except Exception as e:
        print(f"Error resetting counts in Cassandra: {e}")
    finally:
        if cassandra_session:
            cassandra_session.shutdown()
        if cassandra_cluster:
            cassandra_cluster.shutdown()


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

query = startStream()
consumer = KafkaConsumer('job_restart', bootstrap_servers='broker:29092')
for message in consumer:
    msg = json.loads(message.value)
    restart = msg["restart"]
    if restart == "True":
        query.stop()
        query.awaitTermination()
        resetCountsInCassandra()
        query = startStream()
