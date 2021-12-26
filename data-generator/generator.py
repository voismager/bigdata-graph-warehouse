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
    total_posts = 1000

    for i in range(total_users):
        user = {
            "id": f"User_{i}",
            "typeName": "V",
            "className": "User",
            "properties": {}
        }

        print(user)
        producer.send('vk_data', key=bytearray(f"P{i}", 'utf-8'), value=user, partition=0) \
            .add_errback(on_send_error)

    for i in range(total_posts):
        post = {
            "id": f"Post_{i}",
            "typeName": "V",
            "className": "Post",
            "properties": {}
        }

        print(post)
        producer.send('vk_data', key=bytearray(f"P{i}", 'utf-8'), value=post, partition=0) \
            .add_errback(on_send_error)

    for i in range(total_users):
        user_1_id = f"User_{i}"

        for j in range(random.randint(0, 4)):
            while True:
                user_2_id = f"User_{random.randint(0, total_users-1)}"
                if user_1_id != user_2_id:
                    break

            friendof_id = f"FriendOf_{user_1_id}_{user_2_id}"

            edge = {
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

            print(edge)
            producer.send('vk_data', key=bytearray(friendof_id, 'utf-8'), value=edge, partition=0) \
                .add_errback(on_send_error)

    for i in range(total_posts):
        post_id = f"Post_{i}"

        for j in range(random.randint(0, 4)):
            user_id = f"User_{random.randint(0, total_users-1)}"
            likes_id = f"Likes_{user_id}_{post_id}"

            edge = {
                "id": likes_id,
                "typeName": "E",
                "className": "Likes",
                "properties": {
                    "fromId": user_id,
                    "fromClass": "User",
                    "toId": post_id,
                    "toClass": "Post"
                }
            }

            print(edge)
            producer.send('vk_data', key=bytearray(likes_id, 'utf-8'), value=edge, partition=0) \
                .add_errback(on_send_error)

    producer.flush()


if __name__ == '__main__':
    push_data()
