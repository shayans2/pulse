from kafka import KafkaProducer
from kafka.errors import KafkaError
from dotenv import load_dotenv

import json
import logging
import os
import time
import threading
import atexit

# Load env variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaProducerWrapper:
    def __init__(self, bootstrap_servers=os.getenv('KAFKA_SERVER')):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def produce_message(self, topic, message):
        self.producer.send(topic, message)
        self.producer.flush()

kafka_producer = KafkaProducerWrapper()

def send_to_kafka(topic, message):
    try:
        kafka_producer.produce_message(topic, message)
        return "Text sent to Kafka."
    except KafkaError as e:
        logger.exception("Error sending to Kafka", e)
        return "Failed to send to Kafka."

def run_samples(file_path, topic):
    try:
        logger.info(f"Starting to process file: {file_path}")
        with open(file_path, 'r') as file:
            for line in file:
                comment = line.strip()
                if comment:
                    kafka_producer.produce_message(topic, comment)
                    time.sleep(5)
                    
        logger.info(f"Finished processing file: {file_path}")
    except Exception as e:
        logger.exception("Error processing the file", e)
    finally:
            kafka_producer.producer.flush()

# Wrap the call to run_samples in a function that starts it in a new thread
def start_run_samples_in_background(file_path="samples.txt", topic=os.getenv('KAFKA_TOPIC')):
    thread = threading.Thread(target=run_samples, args=(file_path, topic))
    thread.start()
    return "Started processing file in background."

# Define cleanup function
def close_kafka_producer():
    try:
        kafka_producer.producer.close()
        print("Kafka producer closed successfully.")
    except Exception as e:
        print(f"Failed to close Kafka producer: {e}")

atexit.register(close_kafka_producer)
