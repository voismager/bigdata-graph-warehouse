from json import dumps
from kafka import KafkaProducer


def gen_data():
    return [
        {
            "type": "Person",
            "properties": {
                "name": "John",
                "surname": "Doe"
            }
        },
        {
            "type": "Person",
            "properties": {
                "name": "Ann"
            }
        },
        {
            "type": "Person",
            "properties": {
                "name": "Lisa"
            }
        }
    ]


def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)


def on_send_error(err):
    print(err)


def push_data(data):
    producer = KafkaProducer(
        bootstrap_servers=['localhost:29092'],
        key_serializer=lambda x: x,
        value_serializer=lambda x: dumps(x).encode('utf-8'))

    for i, entity in enumerate(data):
        producer.send('vk_data', key=bytearray(str(i), 'utf-8'), value=entity) \
            .add_callback(on_send_success) \
            .add_errback(on_send_error)

    producer.flush()


if __name__ == '__main__':
    push_data(gen_data())
