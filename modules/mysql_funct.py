#!/usr/bin/python3
# Fuctions to access mysql data
# Author Lorenzo Allori <lorenzo.allori@gmail.com>
from modules.configParser_functs import *
import mysql.connector
from mysql.connector import Error

myhost,myport,myuser, mypasswd, mydbname=mysql_properties()

# Test Database connection
def test_connection(): 
    try:
        global mydb
        # Mysql Connection properties
        mydb = mysql.connector.connect(
            host=myhost,
            port=myport,
            user=myuser,
            passwd=mypasswd,
            database=mydbname
            )
        if mydb.is_connected():
            db_Info = mydb.get_server_info()
            print("Connected to MySQL Server version ", db_Info)
            cursor = mydb.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            print("You're connected to database: ", record)
        return mydb
    except Error as e:
        print("Error while connecting to MySQL", e)

def db_connection(): 
    try:
        mydb = mysql.connector.connect(
            host=myhost,
            port=myport,
            user=myuser,
            passwd=mypasswd,
            database=mydbname
            )
        if mydb.is_connected():
            db_Info = mydb.get_server_info()
            cursor = mydb.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
        return mydb
    except Error as e:
        print("Error while connecting to MySQL---NEED SSH TUNNEL?", e)
        exit()


