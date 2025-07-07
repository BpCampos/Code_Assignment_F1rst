import time
import random
from datetime import datetime
from faker import Faker
from quixstreams import Application
import logging
import os
import sys

# Pega o caminho atual que o script está localizado
script_dir = os.path.dirname(os.path.abspath(__file__))

# Cria o nome que o arquivo de log terá
log_file = os.path.join(script_dir, 'producer.log')

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


def generate_iot_data():
    fake = Faker()

    return {
        "device_id": f"sensor_{fake.uuid4()[:8]}",
        "location": fake.city(),
        "ip_address": fake.ipv4_private(),
        "temperature": round(random.uniform(20.0, 30.0), 2),
        "humidity": round(random.uniform(30.0, 60.0), 2),
        "timestamp": datetime.utcnow().isoformat()
    }


def kafka_init():
    try:
        logger.info('Inicializando o broker Kafka')
        app = Application(broker_address="localhost:9092")

        logger.info('Definindo um topico Kafka')
        topic = app.topic(name="iot-data", value_serializer="json")

        logger.info('Gerando dados e enviando para o topico Kafka')
        with app.get_producer() as producer:
            while True:
                logger.info('Gerando dados')
                data = generate_iot_data()
                msg = topic.serialize(key=data["device_id"], value=data)

                # Send to Kafka
                producer.produce(
                    topic=topic.name,
                    key=msg.key,
                    value=msg.value
                )

                print(f"Sent: {data}")
                time.sleep(1)
    except Exception as e:
        logger.error(e)
        sys.exit(1)


if __name__ == '__main__':
    try:
        kafka_init()
    except Exception as e:
        logger.error(e)
        sys.exit(1)
