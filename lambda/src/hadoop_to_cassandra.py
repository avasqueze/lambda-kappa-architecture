from hadoop_map_reduce import PurchaseCount
from cassandra.cluster import Cluster
import time
from kafka import KafkaProducer
import json
from datetime import datetime


def to_cassandra(results):
    """
    Escribe los resultados en Cassandra.

    :param results: Resultados del proceso por lotes, valores contados
    """

    # Creando la sesión
    cassandra_cluster = Cluster(['cassandra1'], port=9042)
    cassandra_session = cassandra_cluster.connect()
    cassandra_session.set_keyspace("all_data_view")

    # Creando la tabla llamada batch_view
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

    # Insertando valores en la tabla, también con UUID y marca de tiempo, solo para validación.
    query = cassandra_session.prepare(query="""
                                      INSERT INTO batch_view
                                      (id, word, counter, time_stamp)
                                      VALUES (uuid(), ?, ?, ?)
                                      """
                                      )

    # Recorriendo los resultados individuales y ejecutando la consulta
    for result in results:
        word = result[0]
        count = int(result[1])
        timestamp = result[2]
        time_stamp = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
        cassandra_session.execute(query, (word, count, time_stamp))


def serializer(message):
    """
    Codifica el mensaje a formato JSON.

    :param message: Mensaje
    """
    return json.dumps(message).encode('utf-8')


args = ["-r", "hadoop", "hdfs:///data.json"]

# Conectando a Kafka
producer = KafkaProducer(bootstrap_servers='broker:29092', value_serializer=serializer)
while True:
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    producer.send('job_restart', {"restart": "True"})
    results = []
    try:
        # escribiendo en el sistema de archivos de Hadoop
        mr_job = PurchaseCount(args=args)
        with mr_job.make_runner() as runner:
            runner.run()
            # Obteniendo los resultados para cada artículo
            for key, value in mr_job.parse_output(runner.cat_output()):
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                results.append((key, value, timestamp))
            # escribiendo los resultados en Cassandra
            to_cassandra(results)
            time.sleep(10)
    except:
        pass