#!/usr/bin/python3.6
# Authors Lorenzo Allori <lorenzo.allori@gmail.con>
# Script that creates EURONEWS index for Elasticsearch 
# These index can be used to create custom visualization dashboards in Kibana
# ver 0.2
# --> Elasticsearch index mapping in "mapping" variable below
# 

import xml.etree.cElementTree as ET
from lxml import etree
from io import StringIO
import xml.dom.minidom
from datetime import datetime
from types import SimpleNamespace
import json
from elasticsearch import Elasticsearch
import uuid
import datetime
import unicodedata
from geopy.geocoders import Nominatim
import os.path
import time

### FILES SECTION
# xml file generated from MIA
xmlFilename='xml-corpus-geo.xml'

### ELASTICSEARCH INDEXES SECTION
# Json filename with ES puts (can be changed according to casestudy)
# jsonFilename='1600experiment.json'
jsonFilename='all_news.json'
# Index name (can be changed according to casestudy) 
#esindex='1600experiment'
esindex='prova2'

# Index mapping
mapping ={
    "mappings":{
      "properties":{
         "newsId":{
            "type":"text"
         },
         "repository":{
            "type":"keyword"
         },
         "collection":{
            "type":"keyword"
         },
         "volume":{
            "type":"keyword"
         },
         "miaDocId":{
            "type":"keyword"
         },
         "hub":{
            "properties":{
               "date":{
                  "type":"date",
                  "format":"dd/MM/yyyy"
               },
               "location":{
                  "type":"geo_point"
               },
               "placeName":{
                  "type":"text"
               }
            }
         },
         "from":{
            "properties":{
               "date":{
                  "type":"date",
                  "format":"dd/MM/yyyy"
               },
               "location":{
                  "type":"geo_point"
               },
               "placeName":{
                  "type":"text"
               }
            }
         },
        #  "plTransit":{
        #     "properties":{
        #     #    "date":{
        #     #       "type":"date",
        #     #       "format":"dd/MM/yyyy"
        #     #    },
        #        "location":{
        #           "type":"geo_point"
        #        },
        #        "placeName":{
        #           "type":"text"
        #        }
        #     }
        #  },
         "transcription":{
            "type":"text", 
            "fielddata": True
         },
         "transcriptionk":{
            "type":"keyword"
         },
         "newsPosition":{
            "type":"text"
         }
      }
   }
}

### Declaring global variables
documentId=int()
miaDocId=""
# newsId is mapDocId+position
newsId=""
repository=""
collection=""
volume=""
hubDate=""
hubPlaceLat=""
hubPlaceLon=""
hubPlaceName=""
hubTranscription=""
fromDate=""
fromPlaceLat=""
fromPlaceLon=""
fromPlaceName=""
plTransitName=""
plTransitLat=""
plTransitLon=""
transcription=""
newsPosition=""
locationDict = {}
longitude = ""
latitude = ""
es=Elasticsearch()
esDoc={}

### FUNCTIONS
# -- UTILS --
def existstr(s):
    return '' if s is None else str(s)

def filldate(s):
    d='/'.join(x.zfill(2) for x in d.split('/'))
    return s

# -- ELASTICSEARCH --
def createIndexElasticsearch():
    # Deleting previous Index
    res = es.indices.delete(index=esindex, ignore=[400,404])
    print('Deleting ES Index '+esindex+' response:'+str(res))
    
    # Creating new Index
    res = es.indices.create(index=esindex, body=mapping, ignore=400)
    print('')
    print('Creating ES Index '+esindex+' response:'+str(res))
    time.sleep(3)

def documentPutElasticsearch():
    global esDoc,documentId, esindex, newsPosition, transcription
    es = Elasticsearch()
    # Define Json Structure
    newsId=miaDocId+"-"+newsPosition
    print(newsId + miaDocId +  fromDate + fromPlaceLat + fromPlaceLon + fromPlaceName)
    # esDoc is defined in the variables section above
    esDoc = {
        "newsId":newsId,
        "miaDocId":miaDocId,
        "repository":repository,
        "collection":collection,
        "volume":volume,
        "hub":{
            "date": hubDate,
            "location": {
                "lat": hubPlaceLat,
                "lon": hubPlaceLon
            },
            "placeName":hubPlaceName
        },
        "from":{
            "date": fromDate,
            "location": {
                "lat": fromPlaceLat,
                "lon": fromPlaceLon
            },
            "placeName":fromPlaceName

        },
        # "plTransit":{
        #     # "date": fromDate,
        #     "location": {
        #         "lat": plTransitLat,
        #         "lon": plTransitLon
        #     },
        #     "placeName":plTransitName

        # },
        "transcription": transcription,
        "transcriptionk": transcription,
        "newsPosition":newsPosition
    }
    res = es.index(index=esindex, id=documentId, body=esDoc)
    print(str(res))
    transcription=""

# -- GEOLOCATIONS --
def loadGeoLocationsFile():
    global locationDict
    if os.path.isfile('./locationDict.txt'):
        dictionary = json.load(open('locationDict.txt'))
        locationDict = dictionary
    else:
        locationDict = {}

def getGeoCoordinates(city):
    global longitude
    global latitude
    global locationDict

    geolocator = Nominatim(user_agent='myapplication')
    if city in locationDict:
        if 'lat' in locationDict[city]:
            latitude=locationDict[city]['lat']
        else:
            pass
        if 'long' in locationDict[city]:
            longitude=locationDict[city]['long']
        else:
            pass
    else:
        #print(city)
        locationDict[city] = {}
        location = geolocator.geocode(city)
        if location != None:    
            latitude=str(location.latitude)
            longitude=str(location.longitude)
            locationDict.setdefault(city, [])
            locationDict[city]['lat'] = latitude
            locationDict[city]['long'] = longitude
        else:
            pass

# Writes a Json file for Elasticsearch (just for test -- this could contain encoding problems)
def writeJson():
    global documentId, newsPosition, transcription
    newsId=miaDocId+"-"+newsPosition
    # Define Json Structure
    if esDoc.get(transcription) is not None:
        print("PRIMA " + esDoc.get(transcription))

    # writing JSON object to file
    with open(jsonFilename, 'a') as f:
        f.write('\n')
        f.write('PUT '+esindex+'/_doc/'+str(documentId))
        json.dump(esDoc, f, indent=4, ensure_ascii=False)
        f.close()


### -- MAIN --
def populateElasticsearchIndex():
    global esDoc,locationDict,documentId,miaDocId,repository,collection,volume,hubDate,hubTranscription,hubPlaceLat,hubPlaceLon,hubPlaceName,newsId,fromDate,fromPlaceLat,fromPlaceLon,fromPlaceName,transcription,newsPosition
    # Inizialize ES index
    createIndexElasticsearch()
    # Start processing XML to get data
    tree = ET.parse(xmlFilename)
    root = tree.getroot()
    documentId=1
    # Loading geo coordinates dictionary is already created
    loadGeoLocationsFile()
    ### MIA doc ID ###
    for document in root.iter('newsDocument'):
        miaDocId=document.find('docid').text
        
        repository=document.find('repository').text
        collection=document.find('collection').text
        volume=document.find('volume').text

        print(miaDocId)
        ### Header section ###
        for header in document.iter('newsHeader'):
            #Get Hub DATE Values
            hubDate=header.find('date')
            print(hubDate)
            if hubDate is None:
                hubDate="01/01/0001"
            else:
                hubDate=header.find('date').text
                if hubDate is None:
                    hubDate="01/01/0001"
                else:
                    hubDate=hubDate.strip()
                    if ' ' in hubDate:
                        hubDate = hubDate.split(' ', 1)[0]
                        print(hubDate)
                    if ';' in hubDate:
                            hubDate = hubDate.replace(";", " ")
                            hubDate = hubDate.split(' ', 1)[0]
                            print(hubDate)
                #fill with zeros the date if wrong format (ex. 6/6/1534 becomes 06/06/1534)
                hubDate='/'.join(x.zfill(2) for x in hubDate.split('/'))
            day,month,year = hubDate.split('/')

            isValidDate = True
            try :
                datetime.datetime(int(year),int(month),int(day))
            except ValueError :
                isValidDate = False
            if(isValidDate) :
                # Date valid
                pass
            else :
                # Date invalid - putting default value - this has to be changed
                hubDate="01/01/0001"
            # Get Hub TRANSCRIPTION
            hubTranscription = header.find('transc')
            if hubTranscription is None:
                print('hubTranscription is none')
                pass
            else:
                hubTranscription=existstr(header.find('transc').text)
                print(hubTranscription)
            #Get Hub PLACES
            print('--start---')
            for x in header.iter('hub'):
                hubPlaceName=x.find('placeName').text
                hubLocation=x.find('location')
                if hubLocation is None:
                    pass
                else:
                    hubPlaceLat=hubLocation.get('lat')
                    hubPlaceLon=hubLocation.get('lon')
                    print("HUB:  "+hubDate+":" +hubPlaceName)
            
            ### From section ###
                newsfrom = header.find('newsFrom')
                if newsfrom is None:
                    ## Documents without news -- only newsHeader is present 
                    if not fromDate:
                        fromDate = hubDate
                    if not fromPlaceLat:
                        fromPlaceLat = hubPlaceLat
                    if not fromPlaceLon:
                        fromPlaceLon = hubPlaceLon
                    if not fromPlaceName:
                        fromPlaceName = hubPlaceName
                    if not transcription:
                        transcription = hubTranscription
                    newsPosition="1"
                    documentPutElasticsearch()
                    print('--end---')
                    documentId=documentId+1
                    print(documentId)
                else:
                    # Documents with news
                    for newsFrom in header.iter('newsFrom'):
                        # Get From PLACES
                        for x in newsFrom.iter('from'):
                            fromPlaceName=x.find('placeName').text
                            if fromPlaceName is None:
                                fromPlaceName = hubPlaceName
                                fromLocation = hubLocation
                                fromPlaceLat = hubPlaceLat
                                fromPlaceLon = hubPlaceLon
                            else:
                                placeList=fromPlaceName.split(',')
                                for x in placeList:
                                    x=x.strip()
                                    if x == "xxx" or x == "[NA]" or x == "NA" or x == "na":
                                        x = hubPlaceName
                                    fromPlaceName=x
                                    getGeoCoordinates(x)
                                    print(x + ": lat "+latitude+" long "+longitude)
                                    fromPlaceLat=latitude
                                    fromPlaceLon=longitude
                                    #Get Date From Values
                                    #---> check if fromDate is none.
                                    i=0
                                    fromDate=newsFrom.find('date')
                                    # print(fromDate)
                                    if fromDate is None:
                                        fromDate=hubDate
                                    else:
                                        fromDate=newsFrom.find('date').text
                                        if fromDate is None:
                                            fromDate=hubDate
                                        else:
                                            print('ENTRA QUI')
                                            fromDate=fromDate.strip()
                                            if ' ' in fromDate:
                                                fromDate.rstrip()
                                                fromDate = fromDate.split(' ', 1)[0]
                                                print(fromDate)
                                            if ';' in fromDate:
                                                fromDate = fromDate.replace(";", " ")
                                                fromDate = fromDate.split(' ', 1)[0]
                                                print(fromDate)
                                            #fill with zeros the date if wrong format (ex. 6/6/1534 becomes 06/06/1534)
                                            fromDate='/'.join(x.zfill(2) for x in fromDate.split('/'))
                                    day,month,year = fromDate.split('/')

                                    isValidDate = True
                                    try :
                                        datetime.datetime(int(year),int(month),int(day))
                                    except ValueError :
                                        isValidDate = False
                                    if(isValidDate) :
                                        # Date valid
                                        pass
                                    else :
                                        # Date invalid - put hub date
                                        # fromDate="01/01/0001"
                                        fromDate=hubDate
                                    
                                    # Get plTransit Values (To be finished)
                                    # for x in newsFrom.iter('plTransit'):
                                    #     plTransitName=x.find('placeName').text
                                    #     if plTransitName is None:
                                    #         plTransitName=""
                                    #     else:
                                    #         placeList=plTransitName.split(',')
                                    #         for x in placeList:
                                    #             x=x.strip()
                                    #             if x == "xxx" or x == "[NA]" or x == "NA" or x == "na":
                                    #                 x = hubPlaceName
                                    #             plTransitName=x
                                    #             getGeoCoordinates(x)
                                    #             print('########PLTRANSIT')
                                    #             print(plTransitName + ": lat "+latitude+" long "+longitude)
                                    #             plTransitLat=latitude
                                    #             plTransitLon=longitude

                                    # Get Trascription Values
                                    transcription=existstr(newsFrom.find('transc').text)
                                    print("TRANSCRIPTION: " + transcription)
                                    # Get Position Value
                                    newsPosition=(newsFrom.find('position'))
                                    if newsPosition is None:
                                        newsPosition="1"
                                    else:
                                        newsPosition=existstr(newsFrom.find('position').text)
                                    writeJson()
                                    documentPutElasticsearch()
                                    print('--end---')
                                    documentId=documentId+1
                                    print(documentId)
                                    # print(locationDict)                     
                            i=i+1
                        


    with open('locationDict.txt', 'w') as f:
        f.write(json.dumps(locationDict))
        f.close()                        


def main():
    print('EURONEWS PROJECT')
    print('Launching ES populating index named: ' + esindex)
    print('')
    print('loading...')
    time.sleep(2)
    populateElasticsearchIndex()

if __name__ == "__main__":
    main()