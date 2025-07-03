import time
import random
from datetime import datetime
from faker import Faker
from quixstreams import Application

# Initialize Faker
fake = Faker()

# Initialize Quix Application
app = Application(broker_address="localhost:9092")

# Define topic with JSON serializer
topic = app.topic(name="iot-data", value_serializer="json")

# Function to generate fake IoT sensor data


def generate_iot_data():
    return {
        "device_id": f"sensor_{fake.uuid4()[:8]}",
        "location": fake.city(),
        "ip_address": fake.ipv4_private(),
        "temperature": round(random.uniform(20.0, 30.0), 2),
        "humidity": round(random.uniform(30.0, 60.0), 2),
        "timestamp": datetime.utcnow().isoformat()
    }


# Get the producer and send data in a loop
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
        time.sleep(1)  # Wait a bit before sending next message
