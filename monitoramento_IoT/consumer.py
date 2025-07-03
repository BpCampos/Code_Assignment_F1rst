from quixstreams import Application
import pyodbc
from dotenv import load_dotenv
import os

load_dotenv()

app = Application(broker_address="localhost:9092")

topic = app.topic(name="iot-data", value_serializer="json")

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost,1433;"
    "DATABASE=iot_data;"
    "UID=sa;"
    f"PWD={os.getenv("SA_PASSWORD")}"
)
cursor = conn.cursor()

with app.get_consumer() as consumer:

    consumer.subscribe([topic.name])
    print(f"Subscribed to topic: {topic.name}")

    while True:
        msg = consumer.poll(1.0)
        if msg is not None:

            value = topic.deserialize(msg)

            key = msg.key().decode() if msg.key() else None

            print(f"[{key}] {value}")
