import time
import json
import random
from data_generator import SnackAutomat
from kafka import KafkaProducer
import logging


def serializer(message):
    """
    Encoding the message into json format.

    :param message: Message
    """
    return json.dumps(message).encode('utf-8')


def snack_to_kafka():
    """
    Generating items and producing them to kafka
    """
    producer = KafkaProducer(
        bootstrap_servers=['broker:9092'],
        value_serializer=serializer
    )
    while True:
        number_of_snack_automats = 125
        snack_id = random.randint(0, number_of_snack_automats)
        snack_automat_id = f"SNACK--{snack_id}"
        for i in SnackAutomat(snack_automat_id=snack_automat_id).get_bought_items():
            producer.send("snack_automat_message", i)
            logging.warning(i)
        time.sleep(1)
if __name__ == '__main__':
    time.sleep(10)
    snack_to_kafka()

