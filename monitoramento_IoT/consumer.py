from quixstreams import Application
import pyodbc
from dotenv import load_dotenv
import os
import logging
import sys

# Pega o caminho atual que o script está localizado
script_dir = os.path.dirname(os.path.abspath(__file__))

# Cria o nome que o arquivo de log terá
log_file = os.path.join(script_dir, 'consumer.log')

# Configuração do logger
logging.basicConfig(
    filename=log_file,
    filemode='w',
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %I:%M:%S',
    level=logging.INFO
)

logger = logging.getLogger(__name__)

load_dotenv()


def insert_to_sql(data):
    logger.info('Adquirindo conexão')
    conn = pyodbc.connect(
        "DRIVER={SQL Server Native Client 11.0};"
        "SERVER=localhost,1433;"
        "DATABASE=master;"
        "UID=sa;"
        f"PWD={os.getenv("SA_PASSWORD")}"
        "Trusted_Connection=no;"
    )
    cursor = conn.cursor()

    # Query para inserir dados no banco
    cursor.execute("""
        INSERT INTO readings (device_id, location, ip_address, temperature, humidity, timestamp)
        VALUES (?, ?, ?, ?, ?, ?)
    """, data["device_id"], data["location"], data["ip_address"],
        data["temperature"], data["humidity"], data["timestamp"])
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

                value = topic.deserialize(msg)

                key = msg.key().decode() if msg.key() else None
                logger.info('Conectando e inserindo dados no banco')
                insert_to_sql(value)
                print(f"[{key}] {value}")


if __name__ == '__main__':
    try:
        run_kafka_consumer()
    except Exception as e:
        logger.warning(e)
        sys.exit(1)
