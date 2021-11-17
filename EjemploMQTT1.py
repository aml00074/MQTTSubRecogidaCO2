import paho.mqtt.client as mqtt
import paho.mqtt.publish as publish
import pytz
import datetime
import pymongo



broker_address = "192.168.1.150"
broker_port = 1883
topic_sub = "medida"
topic_pub0 = "recogida/CO2"
topic_pub1 = "recogida/CO"
topic_pub2 = "recogida/NH4"
topic_pub3 = "recogida/Alcohol"
topic_pub4 = "recogida/Acetona"
lista = []
# MongoDB
uri = 'mongodb://admin:qz3qGzXvu6mZjPkiJ6@asia.ujaen.es:8047/?authSource=admin&authMechanism=SCRAM-SHA-256'
myclient = pymongo.MongoClient(uri)
mydb = myclient["Ejemplo2"]
mycol = mydb["samplesMQ135"]
'''for x in mycol.find().sort("timestamp", -1):
    print(x)'''

def on_message(client, userdata, message):
    mensaje=str(message.payload.decode("utf-8"))
    print("Mensaje recibido=",str(message.payload.decode("utf-8")))
    print("Topic=", message.topic)
    print("Nivel de calidad [0|1|2]=", message.qos)
    print("Flag de retención =", message.retain)

    timestamp = datetime.datetime.now(pytz.timezone('Europe/Madrid'))
    timestamp_str = timestamp.strftime("%d/%m/%Y, %H:%M:%S")
    print("Timestamp=",timestamp_str)
    separado=mensaje.split(';')
    publish.single(topic_pub0, separado[0], hostname="192.168.1.150")
    publish.single(topic_pub1, separado[1], hostname="192.168.1.150")
    publish.single(topic_pub2, separado[2], hostname="192.168.1.150")
    publish.single(topic_pub3, separado[3], hostname="192.168.1.150")
    publish.single(topic_pub4, separado[4], hostname="192.168.1.150")
    print("Mensajes de las medidas recogidas publicados")
    '''dict.append({
        'medidaA0': float(str(message.payload.decode("utf-8"))),
        'timestamp': timestamp_str
    })'''
    list= [{
        'medidaCO2':separado[0],
        'medidaCO':separado[1],
        'medidaNH4':separado[2],
        'medidaAlcohol':separado[3],
        'medidaAcetona':separado[4],
        'timestamp': timestamp_str
    }]
    mycol.insert_many(list)

while(True):
    try:
        client = mqtt.Client('Cliente1')
        client.on_message = on_message
        client.connect(broker_address, broker_port, 60)
        client.subscribe(topic_sub)  # Subscripción al topic_sub
        client.loop_forever()
        ''' para poder ordenar dentro de MongoDB db.getCollection('samplesCO2').find({}).sort({timestamp : 1})'''
    except Exception as e:
        print('e')