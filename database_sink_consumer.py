from kafka import KafkaConsumer
from kafka import TopicPartition, OffsetAndMetadata
import kafka
import mysql.connector
from mysql.connector import Error
import json


class Connect_DB:
    def __init__(self):
        try:
            self.connection = mysql.connector.connect(
                host="localhost", database="..", user="...", password="..."
            )
            if self.connection.is_connected():
                self.cursor = self.connection.cursor()
        except Error as e:
            print(f"Cannot connect to the db {e}")

    def execute_query(self, sql):
        self.cursor.execute(sql)

    def execute_query_select(self, sql):
        self.cursor.execute(sql)
        rows = self.cursor.fetchone()
        offset = int(rows[0])
        return offset

    def commit_data(self):
        self.connection.commit()

    def close_connection(self):
        self.connection.commit()
        self.cursor.close()
        self.connection.close()


class Offsets:
    def __init__(self, db):
        self.offsets = {}
        self.db = db

    def populate(self, partition, offset):
        self.offsets[partition] = offset

    def insert_balance(self, amount):
        self.db.execute_query(f"INSERT INTO DEMO_DATA (AMOUNT) VALUES ({amount})")

    def store_offsets(self, partition):
        self.db.execute_query(
            f"UPDATE OFFSET_TABLE SET LATEST_OFFSET= {self.offsets[partition]} WHERE LATEST_PARTITION= {partition}"
        )
        self.db.commit_data()

    def rewind_offset(self, partition):
        offset = self.db.execute_query_select(
            f"SELECT LATEST_OFFSET FROM OFFSET_TABLE where LATEST_PARTITION = {partition}"
        )
        self.offsets[partition] = offset
        return offset

    def remove_partition(self, partition):
        self.offsets.pop(partition)


class MyConsumerRebalanceListener(kafka.ConsumerRebalanceListener):
    def __init__(self, consumer, offsets_obj):
        self.consumer = consumer
        self.offsets_obj = offsets_obj

    def on_partitions_revoked(self, revoked):
        print("Partitions %s revoked" % revoked)
        try:
            for tp in revoked:
                print("Tp revoked", tp)
                self.offsets_obj.store_offsets(tp.partition)
                self.offsets_obj.remove_partition(tp.partition)
        except Exception as e:
            print("On Revoke exception ", e)

    def on_partitions_assigned(self, assigned):
        print("Partitions %s assigned" % assigned)
        try:
            for tp in assigned:
                print("Tp assigned", tp)
                latest_offset = self.offsets_obj.rewind_offset(tp.partition)
                print("The latest offset is ", latest_offset)
                self.consumer.seek(tp, latest_offset)
        except Exception as e:
            print("On Assign exception:", e)


def process_record(msg, offsets_obj):
    offsets_obj.insert_balance(msg.value)
    offsets_obj.populate(msg.partition, msg.offset + 1)
    offsets_obj.store_offsets(msg.partition)


def main():
    db = Connect_DB()
    topic_name = "...."
    bootstrap_servers = ["localhost:9092"]
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        group_id="groupapo",
        enable_auto_commit=False,
    )

    offsets_obj = Offsets(db)
    listener = MyConsumerRebalanceListener(consumer, offsets_obj)
    consumer.subscribe(topic_name, listener=listener)

    try:
        for message in consumer:
            print("The value is : {}".format(message.value))
            print("The topic is : {}".format(message.topic))
            print("The partition is : {}".format(message.partition))
            print("The offset is : {}".format(message.offset))
            process_record(message, offsets_obj)
            #tp = TopicPartition(message.topic, message.partition)
            #om = OffsetAndMetadata(message.offset + 1, message.timestamp)
            #consumer.commit({tp: om})
            print("*" * 100)
    except Exception as e:
        print("Exception ", e)
        consumer.close()
    finally:
        consumer.close()
        db.close_connection()


if __name__ == "__main__":
    main()
