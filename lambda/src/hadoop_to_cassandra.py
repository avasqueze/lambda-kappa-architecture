from hadoop_map_reduce import PurchaseCount
from cassandra.cluster import Cluster
import time
from kafka import KafkaProducer
import json
from datetime import datetime


def to_cassandra(results):
    cassandra_cluster = Cluster(['cassandra1'], port=9042)
    cassandra_session = cassandra_cluster.connect()
    cassandra_session.set_keyspace("all_data_view")

    cassandra_session.execute(
        """
        CREATE TABLE IF NOT EXISTS batch_view (
            id uuid PRIMARY KEY, 
            word TEXT,
            counter INT,
            time_stamp TIMESTAMP
        )
        """
    )

    query = cassandra_session.prepare(query="""
                                    INSERT INTO batch_view
                                    (id, word, counter, time_stamp)
                                    VALUES (uuid(), ?, ?, ?)
                                  """
                                      )

    for result in results:
        word = result[0]
        count = int(result[1])
        timestamp = result[2]
        time_stamp = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
        cassandra_session.execute(query, (word, count, time_stamp))


def serializer(message):
    return json.dumps(message).encode('utf-8')


args = ["-r", "hadoop", "hdfs:///data.json"]
producer = KafkaProducer(bootstrap_servers='broker:29092', value_serializer=serializer)
while True:
    results = []
    try:
        mr_job = PurchaseCount(args=args)
        with mr_job.make_runner() as runner:
            runner.run()
            for key, value in mr_job.parse_output(runner.cat_output()):
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                results.append((key, value, timestamp))
            to_cassandra(results)
        producer.send('job_restart', {"restart": "True"})
        time.sleep(60)
    except:
        pass
