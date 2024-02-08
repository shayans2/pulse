from kafka import KafkaConsumer, errors
from dotenv import load_dotenv

import pymysql
import joblib
import json
import logging
import os

# Load env variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set up MySQL
class MySQLConnector:
    def __init__(self):
        self.config = {
        'database': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 3306)),
        'charset': os.getenv('DB_CHARSET', 'utf8mb4')
        }
        self.cursor = pymysql.connect(**self.config).cursor
        self.set_encoding()
        self.create_table()

    def set_encoding(self):
        self.cursor().execute("SET NAMES 'utf8';")
        self.cursor().execute("SET CHARACTER SET utf8;")

    def create_table(self):
        self.cursor().execute("""
            CREATE TABLE IF NOT EXISTS classified_comments
            (ID int NOT NULL AUTO_INCREMENT, Comment TEXT, Classified varchar(255), PRIMARY KEY (ID))
            CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        """)

    def insert_data(self, data):
        self.cursor().executemany("""
            INSERT INTO classified_comments (comment, classified) 
            VALUES (%s, %s)
        """, data)


def load_model():
    try:
        classifier = joblib.load('./classifier/cm_classifier.pkl')
        tfidf = joblib.load('./classifier/tf_idf.pkl')
        dic = {1: 'Sad', 0: 'Happy'}
    except Exception as e:
        logger.exception("Failed to load ML models or TFIDF transformer")
        exit(1)

    return classifier, tfidf, dic


def load_consumer(topic):
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=os.getenv('KAFKA_SERVER'),
            auto_offset_reset='earliest',
            group_id='comments_group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
    except errors.NoBrokersAvailable as e:
        logger.exception("No Kafka brokers available")
        exit(1)

    return consumer


batch_size = int(os.getenv('BATCH_SIZE', 1))
batch_data = []

def fetch_and_insert_messages(sql_connector, topic, run_duration_secs):
    consumer = load_consumer(topic)
    classifier, tfidf, dic = load_model()

    with sql_connector.cursor() as cursor:
        for message in consumer:
            try:
                test_message = tfidf.transform([message.value]).toarray()
                prediction = dic[classifier.predict(test_message)[0]]
                logger.info(f"Message: {message.value}, Prediction: {prediction}")
                
                # Add to batch
                batch_data.append((message.value, prediction))
                if len(batch_data) >= batch_size:
                    sql_connector.insert_data(batch_data)
                    cursor.connection.commit()
                    batch_data.clear()  # Clear the batch data after insertion
                    consumer.commit()  # Commit the offsets
                    logger.info(f"Batch inserted and committed, size: {batch_size}")
                    
            except KeyboardInterrupt:
                logger.info("Received KeyboardInterrupt. Closing consumer.")
            finally:
                consumer.commit()# Commit the offset even if there's an error to avoid reprocessing

def kafka_consumer_main():
    mysql_connector = MySQLConnector()

    mysql_connector.set_encoding()
    mysql_connector.create_table()

    topic = os.getenv('KAFKA_TOPIC')
    run_duration_secs = 30

    fetch_and_insert_messages(mysql_connector, topic, run_duration_secs)

if __name__ == '__main__':
    kafka_consumer_main()