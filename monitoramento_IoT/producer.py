import time
import random
from datetime import datetime
from faker import Faker
from quixstreams import Application
import logging
import os

# Pega o caminho atual que o script está localizado
script_dir = os.path.dirname(os.path.abspath(__file__))

# Cria o nome que o arquivo de log terá
log_file = os.path.join(script_dir, 'info.log')

# Configuração do logger
logging.basicConfig(
    filename=log_file,
    filemode='w',
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %I:%M:%S',
    level=logging.INFO
)

logger = logging.getLogger(__name__)

# Inicializa a biblioteca faker para geração de dados ficticios
fake = Faker()

# Inicializando o broker kafka
app = Application(broker_address="localhost:9092")

# Definindo um topico com valores em JSON
topic = app.topic(name="iot-data", value_serializer="json")


# Gerando dicionario com valores ficticios
def generate_iot_data():
    return {
        "device_id": f"sensor_{fake.uuid4()[:8]}",
        "location": fake.city(),
        "ip_address": fake.ipv4_private(),
        "temperature": round(random.uniform(20.0, 30.0), 2),
        "humidity": round(random.uniform(30.0, 60.0), 2),
        "timestamp": datetime.utcnow().isoformat()
    }


# Usa o produtor para enviar dados para o topico Kafka
with app.get_producer() as producer:
    while True:
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
