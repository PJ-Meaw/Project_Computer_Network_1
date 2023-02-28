import mysql.connector

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="password",
    database="mydatabase"
)
# if(mydb) : 
#     print("Connection Successful")

# else :
#     print("Error")

mycursor = mydb.cursor()

# mycursor.execute("SHOW TABLES")

# for x in mycursor:
#   print(x)


