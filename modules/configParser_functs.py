#!/usr/bin/python3
# Fuctions to access config parser
# Author Lorenzo Allori <lorenzo.allori@gmail.com>


from configparser import ConfigParser
import os

def read_configfile():

    configFile="configFile.ini"

    # Get config properties
    myhost=str()
    myport=str()
    myuser=str()
    mypasswd=str()
    mydbname=str()

    if os.path.isfile(configFile):
        parser = ConfigParser()
        parser.read(configFile)
        myhost=parser.get('MYSQLDATABASE', 'mysql.host')
        myport=parser.get('MYSQLDATABASE', 'mysql.port')
        myuser=parser.get('MYSQLDATABASE', 'mysql.user')
        mypasswd=parser.get('MYSQLDATABASE', 'mysql.passwd')
        mydbname=parser.get('MYSQLDATABASE', 'mysql.dbname')
    else:
        print("Config file not found")
    return myhost, myport, myuser, mypasswd, mydbname