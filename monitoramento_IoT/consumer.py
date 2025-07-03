from quixstreams import Application

app = Application(broker_address="localhost:9092")

topic = app.topic(name="iot-data", value_serializer="json")

with app.get_consumer() as consumer:

    consumer.subscribe([topic.name])
    print(f"Subscribed to topic: {topic.name}")

    while True:
        msg = consumer.poll(1.0)
        if msg is not None:

            value = topic.deserialize(msg)

            key = msg.key().decode() if msg.key() else None

            print(f"[{key}] {value}")
