from quixstreams import Application
from faker import Faker

app = Application(broker_address="localhost:9092")

topic = app.topic("dados-sensores-iot")

producer = topic.get_producer()
