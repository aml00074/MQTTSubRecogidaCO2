import paho.mqtt.client as mqtt
import pytz
import datetime
import json
import pymongo

broker_address = "192.168.195.139"
broker_port = 1883
topic = "medida"
lista = []
# MongoDB
uri = 'mongodb://admin:qz3qGzXvu6mZjPkiJ6@asia.ujaen.es:8047/?authSource=admin&authMechanism=SCRAM-SHA-256'
myclient = pymongo.MongoClient(uri)
mydb = myclient["Ejemplo"]
mycol = mydb["samplesCO2"]

def on_message(client, userdata, message):
    print("Mensaje recibido=", str(message.payload.decode("utf-8")))
    print("Topic=", message.topic)
    print("Nivel de calidad [0|1|2]=", message.qos)
    print("Flag de retención =", message.retain)

    timestamp = datetime.datetime.now(pytz.timezone('Europe/Madrid'))
    timestamp_str = timestamp.strftime("%d/%m/%Y, %H:%M:%S")
    print("Timestamp=",timestamp_str)

    '''dict.append({
        'medidaA0': float(str(message.payload.decode("utf-8"))),
        'timestamp': timestamp_str
    })'''
    list= [{
        'medidaA0': float(str(message.payload.decode("utf-8"))),
        'timestamp': timestamp_str
    }]
    mycol.insert_many(list)


client = mqtt.Client('Cliente1')
client.on_message = on_message
client.connect(broker_address, broker_port, 60)
client.subscribe(topic)  # Subscripción al topic
client.loop_forever()
