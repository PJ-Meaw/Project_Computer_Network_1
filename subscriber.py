import paho.mqtt.client as mqtt
import time
import Connect_database

 # define broker address and port for subscriber
BROKER_ADDRESS = "localhost"
PORT = 1883

# function for send data that subscriber received from broker to our database
def send_data(data):

    # sql command for insert data to database
    sql = "INSERT INTO iot (node_id, time_sended, humidity, temperature, thermal_array) VALUES(%s, %s, %s, %s, %s)"
    
    # define data in sql command 
    iotData =  ( data["Node_id"], data["Time_send"], data["Humidity"], data["Temp"], data["Thermal_arr"])
    
    try:
        # executing sql command to database
        Connect_database.mycursor.execute(sql, iotData)
    
        # commit when insert/update data to database
        Connect_database.mydb.commit()
            
        # print to terminal if data inserted
        print("Data inserted")

    except:
        # roll back if error! 
        Connect_database.mydb.rollback()
    

# function that subscriber will do when connecting to broker
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected successfully")
    else:
        print("Connect return result code: " + str(rc))

count = 0
Thermal_arr = []

# function that subscriber will do when connecting to broker
def on_message(client, userdata, message):
    
    # tell program we will use global variable in line 40,41 and variable for data
    global Node_id, Time_send, Humidity, Temp, count, data, Thermal_arr 

    # if topic is node_id keep data -> Node_id
    if message.topic == 'Node_ID':
        Node_id = str(message.payload.decode("utf-8"))
    # if topic is Time_Sent keep data -> Time_send
    if message.topic == 'Time_Sent':
        Time_send = str(message.payload.decode("utf-8"))
    # if topic is Humidity keep data -> Humidity
    if message.topic == 'Humidity':
        Humidity = str(message.payload.decode("utf-8"))
    # if topic is Temperature keep data -> Temperature
    if message.topic == 'Temperature':
        Temp = str(message.payload.decode("utf-8"))

    # if topic is Thermals_array we will looping for received data
    # so we know from data we will split it 16 chunks
    # we will loop and append data in array 16 times
    for i in range (1,16): # for(i=1 ; i<16 ; i++)
        if message.topic == ('Thermals_array/' + str(i)):
            Thermal_arr.append(str(message.payload.decode("utf-8")))
            
    
    # counting number of data that subscriber received    
    count += 1    
    
    # if subscriber get all data from publisher
    # we will send it to send_data() for insert data to database
    if count == 19:
        Thermal = ""
        for i in range (0,15):
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
        # reset count for next message
        count = 0
    
           

# create client variable of mqtt 
client = mqtt.Client()
# when client connect to broker -> do on_connect function
client.on_connect = on_connect
# when client got message from broker -> do on_message function
client.on_message = on_message

# connect this client to broker
client.connect(BROKER_ADDRESS, PORT,200)


# subscribe any topic that subsciber want to received data about that topic
client.subscribe("Node_ID") 
client.subscribe("Time_Sent") 
client.subscribe("Humidity") 
client.subscribe("Temperature")
client.subscribe("Thermals_array/#")

# client.on_message = on_message 



client.loop_forever()
