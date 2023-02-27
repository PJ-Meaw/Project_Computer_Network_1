import paho.mqtt.client as mqtt
import time

BROKER_ADDRESS = "localhost"
PORT = 1883

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected successfully")
    else:
        print("Connect return result code: " + str(rc))


def on_message(client, userdata, msg):

    print("Recieve message: " + msg.topic + "-->" + msg.payload.decode("utf-8"))
           


client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER_ADDRESS, PORT,60)

client.subscribe("NodeId/Time/humidity/temperature/thermal array")

client.loop_forever()
