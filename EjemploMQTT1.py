import paho.mqtt.client as mqtt

broker_address = "192.168.196.139"
broker_port = 1883
topic = "medida"

def on_message(client, userdata, message):
 print("Mensaje recibido=", str(message.payload.decode("utf-8")))
 print("Topic=", message.topic)
 print("Nivel de calidad [0|1|2]=", message.qos)
 print("Flag de retención =", message.retain)

client = mqtt.Client('Cliente1')
client.on_message = on_message
client.connect(broker_address, broker_port, 60)
client.subscribe(topic) # Subscripción al topic
client.loop_forever()
