#!/usr/bin/python3
# Fuctions to access Places on OpenStreetMap
# Author Lorenzo Allori <lorenzo.allori@gmail.com>

import os
import json
import xml.etree.cElementTree as ET
from lxml import etree
from geopy.geocoders import Nominatim
import jsbeautifier

locationDictFile = "locationDict.txt"
comma=","

def loadGeoLocationsFile():
    global locationDict
    if os.path.isfile('./'+locationDictFile):
        dictionary = json.load(open(locationDictFile))
        locationDict = dictionary
    else:
        locationDict = {}

def getGeoCoordinates(city):
    global locationDict
    geolocator = Nominatim(user_agent='euronewsproject.org')
    if city in locationDict:
        if 'lat' in locationDict[city]:
            latitude=locationDict[city]['lat']
        else:
            latitude = ""
        if 'long' in locationDict[city]:
            longitude=locationDict[city]['long']
        else:
            longitude = ""
    else:
        locationDict[city] = {}
        location = geolocator.geocode(city)
        if location != None:    
            latitude=str(location.latitude)
            longitude=str(location.longitude)
            locationDict.setdefault(city, [])
            locationDict[city]['lat'] = latitude
            locationDict[city]['long'] = longitude
        else:
            latitude = ""
            longitude = ""
    return latitude, longitude

