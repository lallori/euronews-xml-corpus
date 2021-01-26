#!/usr/bin/python3
# Fuctions to access config parser
# Author Lorenzo Allori <lorenzo.allori@gmail.com>

from configparser import ConfigParser
import os
import sys

configFile="configFile.ini"
configFileMissing="Config file not found - maybe need to edit configFile_default.ini and rename it to configFile.ini?"

def mysql_properties():
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
        print(configFileMissing)
        sys.exit()
    return myhost, myport, myuser, mypasswd, mydbname

def elastic_properties():
    if os.path.isfile(configFile):
        parser = ConfigParser()
        parser.read(configFile)
        eshost=parser.get('ELASTICSEARCH', 'elasticsearch.host')
        esport=parser.get('ELASTICSEARCH', 'elasticsearch.port')
        esuser=parser.get('ELASTICSEARCH', 'elasticsearch.username')
        espassword=parser.get('ELASTICSEARCH', 'elasticsearch.password')
    else:
        print(configFileMissing)
        sys.exit()
    return eshost, esport, esuser, espassword