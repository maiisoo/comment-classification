import json
import random
import time
from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode("utf-8")


producer = KafkaProducer(bootstrap_servers=[configs.kafka_server_ip],
                         value_serializer=json_serializer)

click_info = []

while 1 == 1:
    mess = {'user_id': '161-2776-3590', 'product_id': 'GSC865238998523', 'source': 'Gmail', 'datetime': '2024-04-22 16:31:11'}
    producer.send("test", mess)
    print("Sent: ", mess)
    time.sleep(1)


