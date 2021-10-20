import paho.mqtt.client as mqtt
import time

broker_address = "192.168.196.139"

client = mqtt.Client('Publicador1')  # Creación del cliente
client.connect(broker_address)
topic = "led"

client.publish(topic, "0")
time.sleep(10)
client.publish(topic, "1")
client.loop_forever()
