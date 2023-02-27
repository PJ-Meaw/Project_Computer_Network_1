import paho.mqtt.client as mqtt
import random
from datetime import datetime
import time
import pandas as pd

BROKER_ADDRESS = "localhost"
PORT = 1883

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected successfully")
    else:
        print("Connect return result code: " + str(rc))


def on_message(client, userdata, msg):
    print("Recieve message: " + msg.topic + "-->" + msg.payload.decode("utf-8"))        


df_films = pd.read_excel('SampleInput.xlsx',"Sheet1")



client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER_ADDRESS, PORT)

# while True:
for index, row in df_films.iterrows():
    print(row)
    data = {
        "node" : "Node1",
        "timesent": str(datetime.now()), 
        "temperature": row.Humidity,
        "humidity": row.Temperature,
        "thermal array": row.ThermalArray
        }

    payload = str(data)
    print(data)
    client.publish("NodeId/Time/humidity/temperature/thermal array", payload)
    time.sleep(10)

