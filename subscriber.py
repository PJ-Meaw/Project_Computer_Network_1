import paho.mqtt.client as mqtt
import time
import Connect_database

BROKER_ADDRESS = "localhost"
PORT = 1883

def stringToInt(string):
    return int(float(string))

def send_data(data):
    sql = "INSERT INTO iot (node_id, time_sended, humidity, temperature, thermal_array) VALUES(%s, %s, %s, %s, %s)"
    
    iotData =  ( data["Node_id"], data["Time_send"], data["Humidity"], data["Temp"], data["Thermal_arr"])
    try:
        # Executing the SQL command
        Connect_database.mycursor.execute(sql, iotData)
    
        # Commit your changes in the database
        Connect_database.mydb.commit()

    except:
        # Rolling back in case of error
        Connect_database.mydb.rollback()

    print("Data inserted")

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected successfully")
    else:
        print("Connect return result code: " + str(rc))

count = 0
Thermal_arr = []
def on_message(client, userdata, message):
    # print("received message: " ,str(message.payload.decode("utf-8")))
    global Node_id, Time_send, Humidity, Temp, count, data, Thermal_arr #ให้ตัวแปรไปเก็บที่บรรทัดที่ 35
    if message.topic == 'Node_ID':
        Node_id = str(message.payload.decode("utf-8"))
    if message.topic == 'Time_Sent':
        Time_send = str(message.payload.decode("utf-8"))
    if message.topic == 'Humidity':
        Humidity = str(message.payload.decode("utf-8"))
    if message.topic == 'Temperature':
        Temp = str(message.payload.decode("utf-8"))
    for i in range (1,17): # for(i=1 ; i<17 ; i++)
        if message.topic == ('Thermals_array/' + str(i)):
            Thermal_arr.append(str(message.payload.decode("utf-8")))
            # print("value" + str(i) + " = " + str(message.payload.decode("utf-8")))
    
        
    count += 1    
    
    if count == 20:
        Thermal = ""
        for i in range (0,16):
            Thermal += Thermal_arr[i]
        print(Thermal)
        data = {
            "Node_id": Node_id,
            "Time_send": Time_send,
            "Humidity": float(Humidity),
            "Temp": float(Temp),
            "Thermal_arr": Thermal
        }
        send_data(data)
        count = 0
    
           


client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER_ADDRESS, PORT,200)

client.subscribe("Node_ID") 
client.subscribe("Time_Sent") 
client.subscribe("Humidity") 
client.subscribe("Temperature")
# client.subscribe("Thermals_array/+")

for i in range (1,17) :
    client.subscribe("Thermals_array/" + str(i))

client.on_message = on_message 



client.loop_forever()
