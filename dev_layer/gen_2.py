#!/usr/bin/env python3

from time import sleep
import datetime
import random
from datetime import timedelta, time
from random import randint, choice
from json import dumps
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

i = 1
while i>0:
    print(("myval", i))
    #producer.send('average', key=b'foo', value=i)
    i +=1
    sleep(2)