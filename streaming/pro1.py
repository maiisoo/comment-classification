import json
import random
import time
from kafka import KafkaProducer

# Hàm để chuyển đổi dữ liệu thành JSON
def json_serializer(data):
    return json.dumps(data).encode("utf-8")

# Khởi tạo KafkaProducer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=json_serializer)

# Hàm để tạo một số tự nhiên ngẫu nhiên
def random_number():
    return random.randint(1, 100)  # Thay đổi giới hạn tùy ý

# Vòng lặp vô hạn để tạo dữ liệu và gửi lên Kafka
while True:
    message = random_number()
    producer.send("test_topic", message)
    print("Sent: ", message)
    time.sleep(1)  # Đợi 1 giây trước khi tạo message mới
