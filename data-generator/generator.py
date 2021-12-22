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

    total_users = 100
    total_posts = 10

    for i in range(total_users):
        user = {
            "id": f"User_{i}",
            "typeName": "V",
            "className": "User",
            "properties": {}
        }

        print(user)
        #producer.send('vk_data', key=bytearray(f"P{i}", 'utf-8'), value=user, partition=0) \
        #    .add_errback(on_send_error)

    for i in range(total_posts):
        post = {
            "id": f"Post_{i}",
            "typeName": "V",
            "className": "Post",
            "properties": {}
        }

        print(post)
        #producer.send('vk_data', key=bytearray(f"P{i}", 'utf-8'), value=post, partition=0) \
        #    .add_errback(on_send_error)

    for i in range(int(total_users / 2)):
        user_1_id = f"User_{i}"
        user_2_id = f"User_{i+1}"
        friendof_id = f"FriendOf_{i}"

        link = {
            "id": friendof_id,
            "typeName": "E",
            "className": "FriendOf",
            "properties": {
                "fromId": user_1_id,
                "fromClass": "User",
                "toId": user_2_id,
                "toClass": "User"
            }
        }

        print(link)
        producer.send('vk_data', key=bytearray(friendof_id, 'utf-8'), value=link, partition=0) \
            .add_errback(on_send_error)

    for i in range(total_posts):
        user_id = f"User_{i}"
        post_id = f"Post_{i}"
        friendof_id = f"Likes_{i}"

        link = {
            "id": friendof_id,
            "typeName": "E",
            "className": "Likes",
            "properties": {
                "fromId": user_id,
                "fromClass": "User",
                "toId": post_id,
                "toClass": "Post"
            }
        }

        print(link)
        producer.send('vk_data', key=bytearray(friendof_id, 'utf-8'), value=link, partition=0) \
            .add_errback(on_send_error)

    producer.flush()


if __name__ == '__main__':
    push_data()
