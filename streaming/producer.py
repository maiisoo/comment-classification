import json
import random
import time
import pandas
from kafka import KafkaProducer
from datetime import datetime


def json_serializer(data):
    return json.dumps(data).encode("utf-8")



# user_id,platform, timestamp, text, post_id,topic, label
df = pandas.read_csv('/home/dis/group1_prj/dataset/users.csv')
click_info = []
topics = ['politics', 'sports', 'music', 'stars', 'education']
platforms = ['instagram', 'facebook', 'twitter', 'linkedin']
texts = ['He slammed the door shut, furious at being ignored once again.',
        'As the thunder roared outside, she huddled under the covers, trembling with fear.',
        'The children laughed and played in the warm sunshine, their joy infectious to everyone around.',
        'With a tender kiss on her forehead, he whispered, "I love you more than words can express."',
        'Tears welled up in her eyes as she read the farewell letter, feeling the weight of loneliness settle in.'
        'Opening the box, she gasped in surprise at finding a beautiful bouquet of flowers inside.',
        'He clenched his fists, seething with anger at the unfairness of the situation.',
        'Walking alone in the dark alley, her heart raced with fear at every sound.',
        'The crowd erupted into cheers as their team scored the winning goal, their joy uncontainable.',
        'With a warm embrace, she held her newborn baby close, feeling an overwhelming rush of love.',
        'Sitting by the window, she watched the rain fall, feeling the heaviness of sadness in her heart.',
        'Turning the corner, he stumbled upon a surprise party thrown by his friends, speechless with astonishment.']

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=json_serializer)
def random_info():
    user_id = random.choice(df['User_id'].tolist())
    platform = platforms[random.randint(0, 3)]
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    post_id = str(random.randint(1, 9999999999))
    text = texts[random.randint(0, 10)]
    topic = topics[random.randint(0, 4)]
    return {'user_id': user_id, 'platform': platform, 'timestamp': timestamp,
            'text': text, 'post_id': post_id, 'topic': topic}


while 1 == 1:
    mess = random_info()
    producer.send("test", mess)
    print("Sent: ", mess)
    time.sleep(1)
