from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, udf
from pyspark.sql.types import StringType, DoubleType, StructType, StructField, IntegerType
import os
from kafka import KafkaConsumer
import json
from cassandra.cluster import Cluster
import uuid


os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'


def generate_uuid():
    """
    Generating UUID for tracking what spark does every step (only needed for validation)

    :return: UUID
    """
    return str(uuid.uuid4())

uuid_udf = udf(generate_uuid, StringType())

def reset_counts_in_cassandra():
    """
    For Lambda it is needed to reset all the values in the Cassandra.
    """

    cassandra_cluster = None
    cassandra_session = None
    try:
        # Trying to connect
        cassandra_cluster = Cluster(['cassandra1'], port=9042)
        cassandra_session = cassandra_cluster.connect()
        cassandra_session.set_keyspace("all_data_view")

        items = ['apple', 'banana', 'orange', 'blueberries', 'mars', 'twix', 'snickers', 'milkyway']

        # Resetting values
        for item in items:
            cassandra_session.execute(
                f"INSERT INTO real_time_view (id, item, count, time_stamp) VALUES (uuid(), '{item}', 0, toTimestamp(now()))")
    except Exception as e:
        print(f"Error resetting counts in Cassandra: {e}")
    finally:
        # Closing session
        if cassandra_session:
            cassandra_session.shutdown()
        if cassandra_cluster:
            cassandra_cluster.shutdown()


def write_to_cassandra(batch_df, batch_id):
    """
    Writing every batch to Cassandra
    """

    cassandra_cluster = None
    cassandra_session = None
    try:
        # Trying to connect
        cassandra_cluster = Cluster(['cassandra1'], port=9042)
        cassandra_session = cassandra_cluster.connect()
        cassandra_session.set_keyspace("all_data_view")

        # Creating a table named real_time_view, where the results from spark will be stored at.
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

        # Adding columns
        batch_df_with_timestamp_and_uuid = batch_df \
            .withColumn("time_stamp", current_timestamp()) \
            .withColumn("id", uuid_udf())

        # Writing to cassandra directly via connector
        batch_df_with_timestamp_and_uuid.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="real_time_view", keyspace="all_data_view") \
            .mode("append") \
            .save()
    except Exception as e:
        print(f"Error writing to Cassandra: {e}")
    finally:
        # If process is done, disconnecting from cassandra.
        if cassandra_session:
            cassandra_session.shutdown()
        if cassandra_cluster:
            cassandra_cluster.shutdown()

def start_stream():
    """
    Starting the spark session. Grouping and counting items.
    """

    # Setting up Spark
    spark = SparkSession.builder \
        .appName("KafkaStreamProcessing") \
        .config("spark.cassandra.connection.host", "cassandra1") \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.master", os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077")) \
        .getOrCreate()

    # Defining the data structure (like columns of the table and dtypes)
    schema = StructType([
        StructField("item", StringType()),
        StructField("customer_id", StringType()),
        StructField("healthy_food", StringType()),
        StructField("price", DoubleType()),
        StructField("snack_automat_id", StringType()),
        StructField("timestamp", StringType())
    ])

    # Getting messages directly from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("subscribe", "snack_automat_message") \
        .option("startingOffsets", "latest") \
        .load()

    # Grouping and counting
    parsed_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")
    result_df = parsed_df.groupBy("item").count()

    # Writing cassandra
    query = result_df.writeStream \
        .trigger(processingTime="2 seconds") \
        .outputMode("complete") \
        .foreachBatch(write_to_cassandra) \
        .option("spark.cassandra.connection.host", "cassandra1") \
        .start()

    return query

query = start_stream()
# If batch is done, resetting values
consumer = KafkaConsumer('job_restart', bootstrap_servers='broker:29092')
for message in consumer:
    msg = json.loads(message.value)
    restart = msg["restart"]
    if restart == "True":
        query.stop()
        query.awaitTermination()
        reset_counts_in_cassandra()
        query = start_stream()
