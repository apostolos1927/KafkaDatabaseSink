from time import sleep
from json import dumps
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import random


topic_name = "...."
partitions = 2
replication = 1
admin_client = KafkaAdminClient(bootstrap_servers=["localhost:9092"])

print("List topics:", admin_client.list_topics())

if topic_name not in admin_client.list_topics():
    new_topic = NewTopic(
        name=topic_name, num_partitions=partitions, replication_factor=replication
    )
    admin_client.create_topics(new_topics=[new_topic])


producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda x: dumps(x).encode("utf-8"),
)

for e in range(1000):
    amount = random.randint(1, 100)
    print(amount)
    producer.send(topic_name, value=amount)
    sleep(2)
