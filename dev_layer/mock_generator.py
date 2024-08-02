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

temperature1 = random.uniform(14, 33)
temperature2 = random.uniform(14, 33)
dt = datetime.datetime.today()

def th1(time, temperature1):
    diff = random.uniform(0, 2)
    k = random.randint(0, 1)
    if k == 0:
        temperature1 = temperature1 + diff
    else:
        temperature1 = temperature1 - diff
    if temperature1 > 35:
        temperature1 = temperature1 - 5
    if temperature1 < 12:
        temperature1 = temperature1 + 5
    temperature1 = round(temperature1, 1)
    time = datetime.datetime.strftime(time , '%Y-%m-%d %H:%M')
    strtime1 = "TH1 "+str(time)+" | " + str(temperature1)
    print("TH1", time, "|", temperature1)
    return (temperature1, strtime1)

def th2(time, temperature2):
    diff = random.uniform(0, 2)
    k = random.randint(0, 1)
    if k == 0:
        temperature2 = temperature2 + diff
    else:
        temperature2 = temperature2 - diff
    if temperature2 > 35:
        temperature2 = temperature2 - 5
    if temperature2 < 12:
        temperature2 = temperature2 + 5
    temperature2 = round(temperature2, 1)
    time = datetime.datetime.strftime(time , '%Y-%m-%d %H:%M')
    strtime2 = "TH2 "+str(time)+" | "+str(temperature2) 
    print("TH2", time, "|", temperature2)
    return (temperature2, strtime2)

def hvac1(time):
    cons1 = random.uniform(0, 100)
    cons1 = round(cons1, 1)
    time = datetime.datetime.strftime(time , '%Y-%m-%d %H:%M')
    strhvac1 = "HVAC1 "+time+" | "+str(cons1)
    print("HVAC1", time, "|", cons1)
    return (cons1, strhvac1)

def hvac2(time):
    cons2 = random.uniform(0,200)
    cons2 = round(cons2, 1)
    time = datetime.datetime.strftime(time , '%Y-%m-%d %H:%M')
    strhvac2 = "HVAC2 "+time+" | "+str(cons2)
    print("HVAC2", time, "|", cons2)
    return (cons2, strhvac2)

def miac1(time):
    mi_cons1 = random.uniform(0, 150)
    mi_cons1 = round(mi_cons1, 1)
    time = datetime.datetime.strftime(time , '%Y-%m-%d %H:%M')
    strmiac1 = "MiAC1 "+time+" | "+str(mi_cons1)
    print("MiAC1", time, "|", mi_cons1)
    return (mi_cons1, strmiac1)

def miac2(time):
    mi_cons2 = random.uniform(0, 200)
    mi_cons2 = round(mi_cons2, 1)
    time = datetime.datetime.strftime(time , '%Y-%m-%d %H:%M')
    strmiac2 = "MiAC1 "+time+" | "+str(mi_cons2)
    print("MiAC2", time, "|", mi_cons2)
    return (mi_cons2, strmiac2)

def w1(time):
    water_cons = random.uniform(0, 1)
    water_cons = round(water_cons, 1)
    time = datetime.datetime.strftime(time , '%Y-%m-%d %H:%M')
    strwatercons = "W1 "+time+" | "+str(water_cons)
    print("W1", time, "|", water_cons)
    return (water_cons, strwatercons)

def mov1(time):
    time = datetime.datetime.strftime(time , '%Y-%m-%d %H:%M')
    strmov1 = "Mov1 "+time+" | 1"
    print("Mov1", time, "| 1")
    return strmov1

def etot(time, energy):
    #time = datetime.datetime.strftime(time , '%Y-%m-%d 00:00')
    time = datetime.datetime.strftime(time , '%Y-%m-%d %H:00')
    k = random.randint(-1000, 1000)
    energy = energy + 62400 + k
    strenergy = "Etot "+time+" | "+str(energy)
    print("Etot", time, "|", energy)
    return (energy, strenergy)

def wtot(time, water_tot):
    time = datetime.datetime.strftime(time , '%Y-%m-%d 00:00')
    k = random.randint(-10, 10)
    water_tot = water_tot + 110 + k
    strwater_tot = "Wtot "+time+" | "+str(water_tot)
    print("Wtot", time, "|", water_tot)
    return (water_tot, strwater_tot) 

water_tot = 0
energy_tot = 0
cnt = 0
mov_cnt = 0
time_old = 0
msg_id = 0

while 1:   
    if cnt >= 96:
        cnt=0 
    
    dt = dt + timedelta(minutes=15) 
    print(dt)
    print(time_old)
    (temperature1, strtime1) = th1(dt, temperature1) 
    (temperature2, strtime2) = th2(dt, temperature2)

    tmp_id = str(msg_id)
    producer.send('average', key=tmp_id.encode('ascii'), value=strtime1)
    msg_id += 1
    tmp_id = str(msg_id)
    producer.send('average', key=tmp_id.encode('ascii'), value=strtime2)
    msg_id += 1

    (cons1, strhvac1) = hvac1(dt)
    (cons2, strhvac2) = hvac2(dt)

    tmp_id = str(msg_id)
    producer.send('sum', key=tmp_id.encode('ascii'), value=strhvac1)
    msg_id += 1
    tmp_id = str(msg_id)
    producer.send('sum', key=tmp_id.encode('ascii'), value=strhvac2)
    msg_id += 1

    (mi_cons1, strmiac1) = miac1(dt)
    (mi_cons2, strmiac2) = miac2(dt)

    tmp_id = str(msg_id)
    producer.send('sum', key=tmp_id.encode('ascii'), value=strmiac1)
    msg_id += 1
    tmp_id = str(msg_id)
    producer.send('sum', key=tmp_id.encode('ascii'), value=strmiac2)
    msg_id += 1

    h = int(dt.strftime("%H"))
    m = int(dt.strftime("%M"))

    if (h==0) and (m < 15):
        (energy_tot, strenergy) = etot(dt, energy_tot)
        tmp_id = str(msg_id)
        producer.send('max', key=tmp_id.encode('ascii'), value=strenergy)
        msg_id += 1
        (water_tot, strwater_tot) = wtot(dt, water_tot) 
        tmp_id = str(msg_id)
        producer.send('max', key=tmp_id.encode('ascii'), value=strwater_tot)
        msg_id += 1

    if cnt > 90 and mov_cnt<4:
        strmov1 = mov1(dt)
        mov_cnt = mov_cnt + 1
        tmp_id = str(msg_id)
        producer.send('sum', key=tmp_id.encode('ascii'), value=strmov1)
        msg_id += 1
    elif (random.choices([0,1], weights=[95, 5])[0] == 1):
        strmov1 = mov1(dt)
        mov_cnt = mov_cnt + 1
        tmp_id = str(msg_id)
        producer.send('sum', key=tmp_id.encode('ascii'), value=strmov1)
        msg_id += 1 
        
    (water_cons, strwatercons) = w1(dt)
    tmp_id = str(msg_id)
    producer.send('sum', key=tmp_id.encode('ascii'), value=strwatercons) 
    msg_id += 1
    
    time_old = time_old + 1

    if (time_old % 20 == 0):
        dtold2days = dt -  timedelta(days=2) 
        (w1old2days, strw1old2days)  = w1(dtold2days)
        tmp_id = str(msg_id)
        producer.send('sum', key=tmp_id.encode('ascii'), value=strw1old2days)
        msg_id += 1

    if (time_old % 120 == 0):
        dtold10days = dt -  timedelta(days=10) 
        (w1old10days, strw1old10days) = w1(dtold10days)
        tmp_id = str(msg_id)
        producer.send('sum', key=tmp_id.encode('ascii'), value=strw1old10days)
        msg_id += 1
        
    cnt = cnt + 1
    
    sleep(1)

