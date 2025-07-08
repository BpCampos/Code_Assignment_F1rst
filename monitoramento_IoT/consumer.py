from quixstreams import Application
import psycopg2
from dotenv import load_dotenv
import os
import logging
import sys
from datetime import datetime
import json

# Pega o caminho atual que o script está localizado
script_dir = os.path.dirname(os.path.abspath(__file__))

# Cria o nome que o arquivo de log terá
log_file = os.path.join(script_dir, 'consumer.log')

# Configuração do logger
logging.basicConfig(
    filename=log_file,
    filemode='w',
    encoding='utf-8',
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %I:%M:%S',
    level=logging.INFO
)

logger = logging.getLogger(__name__)

load_dotenv()

try:
    logger.info('Adquirindo conexao com SQL Server')
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )
    cursor = conn.cursor()
except Exception as e:
    logger.error(e)
    sys.exit(1)


def insert_to_sql(data):
    # Query para inserir dados no banco
    cursor.execute("""
            INSERT INTO readings (device_id, location, ip_address, temperature, humidity, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
        data["device_id"],
        data["location"],
        data["ip_address"],
        float(data["temperature"]),
        float(data["humidity"]),
        datetime.fromisoformat(data["timestamp"].replace("Z", "+00:00"))
    ))
    conn.commit()


def run_kafka_consumer():
    logger.info('Acessando o topico no Kafka')
    app = Application(broker_address="localhost:9092")

    logger.info('Se inscrevendo no topico Kafka')
    topic = app.topic(name="iot-data", value_serializer="json")

    logger.info('Consumindo dados do topico')
    with app.get_consumer() as consumer:
        consumer.subscribe([topic.name])
        logger.info(f"Subscrito no topico: {topic.name}")

        while True:
            msg = consumer.poll(1.0)
            if msg is not None:

                data = json.loads(msg.value())

                key = msg.key().decode() if msg.key() else None
                logger.info('Conectando e inserindo dados no banco')
                insert_to_sql(data)
                logger.info(f"[{key}] {data}")


if __name__ == '__main__':
    try:
        run_kafka_consumer()
    except Exception as e:
        logger.error(e)
        sys.exit(1)
