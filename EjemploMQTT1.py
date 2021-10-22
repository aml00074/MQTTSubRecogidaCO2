import paho.mqtt.client as mqtt
import pytz
import datetime
import json
broker_address = "192.168.195.139"
broker_port = 1883
topic = "medida"
dict = {}
dict['CO2'] = []
# print(pytz.all_timezones) #España es 'Europe/Madrid',

def on_message(client, userdata, message):
    print("Mensaje recibido=", str(message.payload.decode("utf-8")))
    print("Topic=", message.topic)
    print("Nivel de calidad [0|1|2]=", message.qos)
    print("Flag de retención =", message.retain)
    print("Timestamp=",datetime.datetime.now(pytz.timezone('Europe/Madrid')))
    timestamp = datetime.datetime.now(pytz.timezone('Europe/Madrid'))
    dict['CO2'].append({
        'medidaA0': str(message.payload.decode("utf-8")),
        'timestamp': [timestamp.day, timestamp.month, timestamp.year, timestamp.hour, timestamp.minute,
                      timestamp.second]
    })
    print(dict)
    with open('dict.json', 'w') as file:
        json.dump(dict, file, indent=4)



client = mqtt.Client('Cliente1')
client.on_message = on_message
client.connect(broker_address, broker_port, 60)
client.subscribe(topic)  # Subscripción al topic
client.loop_forever()
