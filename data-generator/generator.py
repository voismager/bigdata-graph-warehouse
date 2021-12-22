from json import dumps
from kafka import KafkaProducer
import random


def on_send_error(err):
    print(err)


def push_data():
    producer = KafkaProducer(
        bootstrap_servers=['localhost:29092'],
        key_serializer=lambda x: x,
        value_serializer=lambda x: dumps(x).encode('utf-8'))

    for i in range(50000):
        entity = {
            "id": f"P{i}",
            "typeName": "V",
            "className": "User",
            "properties": {
            }
        }

        producer.send('vk_data', key=bytearray(f"P{i}", 'utf-8'), value=entity) \
            .add_errback(on_send_error)

        if i > 10 and i % 2 == 0:
            first = random.randint(0, i)
            second = random.randint(0, i)
            while first == second:
                second = random.randint(0, i)

            link = {
                "id": f"E{i}",
                "typeName": "E",
                "className": "FriendOf",
                "properties": {
                    "fromId": f"P{first}",
                    "toId": f"P{second}"
                }
            }

            #producer.send('vk_data', key=bytearray(f"E{i}", 'utf-8'), value=link, partition=random.randint(0, 1)) \
            #    .add_errback(on_send_error)

    producer.flush()


if __name__ == '__main__':
    push_data()
