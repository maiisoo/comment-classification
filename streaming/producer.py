import json
import random
import time
import pandas
from kafka import KafkaProducer
from datetime import datetime


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=json_serializer)

# user_id,platform, timestamp, text, post_id,topic, label
df = pandas.read_csv('../dataset/users.csv')
click_info = []
topics = ['politics', 'sports']
platforms = ['instagram', 'facebook', 'twitter', 'linkedin']
texts = ['a love b', 'a love c', 'a love d', 'a love e', 'a love f', 'a love g', 'a love h', 'a love i', 'a love j',
         'a love k', 'a love l']


def random_info():
    user_id = random.choice(df['User_id'].tolist())
    platform = platforms[random.randint(0, 3)]
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    post_id = str(random.randint(1, 9999999999))
    text = texts[random.randint(0, 10)]
    topic = topics[random.randint(0, 1)]
    return {'user_id': user_id, 'platform': platform, 'timestamp': timestamp,
            'text': text, 'post_id': post_id, 'topic': topic}


while 1 == 1:
    mess = random_info()
    producer.send("test", mess)
    print("Sent: ", mess)
    time.sleep(1)
