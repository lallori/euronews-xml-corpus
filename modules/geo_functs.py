#!/usr/bin/python3
# Fuctions to access Places on OpenStreetMap
# Author Lorenzo Allori <lorenzo.allori@gmail.com>

import os
import json
import xml.etree.cElementTree as ET
from lxml import etree
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

import jsbeautifier

locationDictFile = "locationDict.txt"
comma=","

def loadGeoLocationsFile():
    global locationDict
    if os.path.isfile('./'+locationDictFile):
        try:
            locationDict = json.load(open(locationDictFile))
        except ValueError as e:
            print('file does not contain json - recreating it')
            locationDict = {}
    else:
        locationDict = {}
    return locationDict

def prettyJsonLocationDict():
    # Parse it
    formattedJson = jsbeautifier.beautify_file(locationDictFile)
    with open(locationDictFile, 'w') as f:
        f.write(formattedJson)
        f.close()

def getGeoCoordinates(city):
    locationDict = loadGeoLocationsFile()
    geolocator = Nominatim(user_agent='euronewsproject.org')
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

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
        # location = geolocator.geocode(city) #if you want to use it without delay (min_delay_seconds=1)
        location = geocode(city)
        if location != None:    
            latitude=str(location.latitude)
            longitude=str(location.longitude)
            locationDict.setdefault(city, [])
            locationDict[city]['lat'] = latitude
            locationDict[city]['long'] = longitude
        else:
            latitude = ""
            longitude = ""
    
        # Update location dictionary file
        with open(locationDictFile, 'w') as f:
            f.write(json.dumps(locationDict))
            f.close()
    
    prettyJsonLocationDict()

    return latitude, longitude

