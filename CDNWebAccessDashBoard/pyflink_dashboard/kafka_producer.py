from kafka import KafkaProducer


class KafkaMsgProducer:
    def __init__(self, server, topic):
        self._server = server
        self.producer = None
        self.topic = topic

    def connect(self):
        if self.producer is None:
            producer = KafkaProducer(bootstrap_servers=self._server)
            self.producer = producer

    def close(self):
        if self.producer is not None:
            self.producer.close()
            self.producer = None

    def send(self, msg):
        if self.producer is not None:
            if not isinstance(msg, bytes):
                msg = msg.encode("utf-8")  # 将str类型转换为bytes类型
            self.producer.send(topic=self.topic, value=msg)


def get_kafka_producer(port, topic):
    producer = KafkaMsgProducer("localhost:%d" % port, topic)
    producer.connect()  # 建立连接
    return producer
