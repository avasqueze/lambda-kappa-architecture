from cassandra.cluster import Cluster
from kafka import KafkaConsumer
import json
import datetime
import logging


def run_kafka_to_cassandra():
    cassandra_cluster = Cluster(['cassandra1'], port=9042)
    cassandra_session = cassandra_cluster.connect()
    cassandra_session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS all_data_view
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
        """
    )
    cassandra_session.set_keyspace("all_data_view")

    cassandra_session.execute(
        """
        CREATE TABLE IF NOT EXISTS all_data_view (
            id uuid PRIMARY KEY,
            item VARCHAR,
            customer_id INT, 
            healthy_food VARCHAR,
            price FLOAT,
            snack_automat_id VARCHAR, 
            time_stamp TIMESTAMP,
            ones INT
        )
        """
    )

    # Kafka consumer
    consumer = KafkaConsumer(
        "snack_automat_message",
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest'
    )


    def process_message_and_insert_into_cassandra(session, message):
        # Assume message is a JSON string

        # Extract relevant fields from the message
        customer_id = message.get('customer_id')
        item = message.get('item')
        snack_automat_id = message.get('snack_automat_id')
        timestamp = message.get('timestamp')
        timestamp = datetime.datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
        healthy_food = message.get('healthy_food')
        price = message.get('price')
        ones = message.get('ones')

        # Construct and execute a Cassandra query to insert the data
        query = session.prepare(query="""
                                        INSERT INTO all_data_view
                                        (id, item,customer_id, healthy_food, price, snack_automat_id, time_stamp, ones)
                                        VALUES (uuid(), ?, ?, ?, ?, ?, ?, ?)
                                      """)
        session.execute(query, (item, customer_id, healthy_food, price, snack_automat_id, timestamp, ones))


    for message in consumer:
        msg = json.loads(message.value)
        logging.warning(msg)
        process_message_and_insert_into_cassandra(cassandra_session, msg)

if __name__ == '__main__':
    run_kafka_to_cassandra()
