import json

from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "test",
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True)

print("starting the consumer")
for msg in consumer:
    print("Click = {}".format(json.loads(msg.value)))