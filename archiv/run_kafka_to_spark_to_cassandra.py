# Spark Container
from kafka import KafkaConsumer
import subprocess
import json

# Initialize Kafka consumer
consumer = KafkaConsumer('job_restart', bootstrap_servers='broker:29092')

# Listen for messages
for message in consumer:
    msg = json.loads(message.value)
    restart = msg["restart"]
    if restart == "True":
        subprocess.run(['spark-submit',
                        '--packages',
                        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.4.0",
                        "src/kafka_to_spark_to_cassandra.py",
                        "broker:29092"])




