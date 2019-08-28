# -*- coding: UTF-8 -*-

from kafka import KafkaProducer
from kafka import KafkaAdminClient
from kafka import KafkaConsumer
from kafka.admin import NewTopic
import json


def send_msg(topic='test', msg=None):
    producer = KafkaProducer(bootstrap_servers='localhost:9092',
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    if msg is not None:
        future = producer.send(topic, msg)
        future.get()


def get_msg(topic='test'):
    consumer = KafkaConsumer(topic, auto_offset_reset='earliest')
    for message in consumer:
        print(message)


def list_topics():
    global_consumer = KafkaConsumer(bootstrap_servers='localhost:9092')
    topics = global_consumer.topics()
    return topics

def create_topic(topic='test'):
    admin = KafkaAdminClient(bootstrap_servers='localhost:9092')
    topics = list_topics()
    if topic not in topics:
        topic_obj = NewTopic(topic, 1, 1)
        admin.create_topics(new_topics=[topic_obj])

# 删除Topic
def delete_topics(topic='test'):
    admin = KafkaAdminClient(bootstrap_servers='localhost:9092')
    topics = list_topics()
    if topic in topics:
        admin.delete_topics(topics=[topic])


if __name__ == '__main__':
    topic = 'user'
    topics = list_topics()
    if topic in topics:
        delete_topics(topic)

    create_topic(topic)
    msgs = [{'a': 'a', 'b': 1, 'c': 1, 'time': '2013-01-01T00:14:13Z'},
            {'a': 'b', 'b': 2, 'c': 2, 'time': '2013-01-01T00:24:13Z'},
            {'a': 'a', 'b': 3, 'c': 3, 'time': '2013-01-01T00:34:13Z'},
            {'a': 'a', 'b': 4, 'c': 4, 'time': '2013-01-01T01:14:13Z'},
            {'a': 'b', 'b': 4, 'c': 5, 'time': '2013-01-01T01:24:13Z'},
            {'a': 'a', 'b': 5, 'c': 2, 'time': '2013-01-01T01:34:13Z'}]
    for msg in msgs:
        send_msg(topic, msg)
    # print test data
    get_msg(topic)
