import mysql.connector


# detail for our database that we want to keep data.
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="password",
    database="mydatabase"
) 

# print status of connection to terminal
if(mydb) : 
    print("Connection Successful")

else :
    print("Error")


# define cursor for execute data to database
mycursor = mydb.cursor()



