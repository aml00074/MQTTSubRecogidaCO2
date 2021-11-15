from typing import List

import paho.mqtt.client as mqtt
import pytz
import datetime
import pymongo
import json


broker_address = "192.168.1.150"
broker_port = 1883
topic = "#"
lista = []
# MongoDB
uri = 'mongodb://admin:qz3qGzXvu6mZjPkiJ6@asia.ujaen.es:8047/?authSource=admin&authMechanism=SCRAM-SHA-256'
myclient = pymongo.MongoClient(uri)
mydb = myclient["Ejemplo2"]
mycol = mydb["samplesMQ135"]
print('hi')
'''for x in mycol.find().sort("timestamp", -1):
    print(x)'''

def on_message(client, userdata, message):
    print('hi3')
    mensaje=str(message.payload.decode("utf-8"))
    print("Mensaje recibido=",str(message.payload.decode("utf-8")))
    print("Topic=", message.topic)
    print("Nivel de calidad [0|1|2]=", message.qos)
    print("Flag de retención =", message.retain)

    timestamp = datetime.datetime.now(pytz.timezone('Europe/Madrid'))
    timestamp_str = timestamp.strftime("%d/%m/%Y, %H:%M:%S")
    print("Timestamp=",timestamp_str)
    #separado=mensaje.split(';')
    #print(separado)

    '''dict.append({
        'medidaA0': float(str(message.payload.decode("utf-8"))),
        'timestamp': timestamp_str
    })'''
    list = [{
        'medidaCO2': 0,
        'medidaCO': 1,
        'medidaNH4': 2,
        'medidaAlcohol': 3,
        'medidaAcetona':4,
        'timestamp': timestamp_str
    }]
    '''list= [{
        'medidaCO2':separado[0],
        'medidaCO':separado[1],
        'medidaNH4':separado[2],
        'medidaAlcohol':separado[3],
        'medidaAcetona':separado[4],
        'timestamp': timestamp_str
    }]'''
    mycol.insert_many(list)

while(True):
    try:
        client = mqtt.Client('Cliente1')
        client.on_message = on_message
        client.connect(broker_address, broker_port, 60)
        print('hi1.5')
        client.subscribe(topic)  # Subscripción al topic
        print('hi2')
        client.loop_forever()
        print('hi4')
        ''' para poder ordenar dentro de MongoDB db.getCollection('samplesCO2').find({}).sort({timestamp : 1})'''
    except Exception as e:
        print('e')