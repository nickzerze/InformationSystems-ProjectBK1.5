from kafka import KafkaConsumer
#from pymongo import MongoClient
from json import loads
from time import sleep

consumer = KafkaConsumer(
    'average',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

""" for message in consumer:
     message value and key are raw bytes -- decode if necessary!
     e.g., for unicode: `message.value.decode('utf-8')`
     print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                         message.value)) """

consumer.subscribe(['average'])

for message in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    print (message.value)

