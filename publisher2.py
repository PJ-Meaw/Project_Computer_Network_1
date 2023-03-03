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

client.connect(BROKER_ADDRESS, PORT,200)

Node_id = "0002"

# while True:
for index, row in df_films.iterrows(): 
    #print(row)
    client.publish("Node_ID",Node_id) # TOPIC is NODE_ID
    #print("Just published " + str(Node_id) + " to topic TEMPERATURE")
    client.publish("Time_Sent",str(datetime.now())) # TOPIC is Time
    #print("Just published " + str(datetime.now()) + " to topic Time_Sent")
    client.publish("Humidity",row.Humidity) # TOPIC is Time
    # print("Just published " + str(row.Humidity) + " to topic Humidity")
    client.publish("Temperature",row.Temperature) # TOPIC is Time
    # print("Just published " + str(row.Temperature) + " to topic Temperature")
    i = 1
    value = len(row.ThermalArray)
    while(value > 0) :
        if(value > 250) : 
            info = row.ThermalArray
            client.publish("Thermals_array/" + str(i), info[0:250]) # TOPIC is Time
            print(info[0:250])
            row.ThermalArray = row.ThermalArray[250:len(row.ThermalArray)]
            print("Thermals_array/" + str(i))
            
            value -= 250
            # print("Just published " + str(row.ThermalArray) + " to topic Thermal_array") 
            print(i)
        else : 
            info = row.ThermalArray
            client.publish("Thermals_array/" + str(i), info[0:value]) # TOPIC is Time
            print(info[0:value])
            print("Thermals_array/" + str(i))
            value -= len(info)
            print(i)
        
        i += 1
        
    time.sleep(10) # 2 minutes for read next IOT NODE

