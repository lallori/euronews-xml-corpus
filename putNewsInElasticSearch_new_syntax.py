#!/usr/bin/python3
# Authors Lorenzo Allori <lorenzo.allori@gmail.con>
# Script that creates EURONEWS index for Elasticsearch 
# These index can be used to create custom visualization dashboards in Kibana
# ver 1.0
# ElasticSearch functions are in ./modules/elastic_funct.py
# This can be used only if we have <from> tag with the new syntax (with date attributes etc.)
# To be tested
# N.B. In Kibana, if you go to the Index pattern in Settings or Management 
# you can set the format for your URL to be URL. That makes it a clickable link in Discover. 
# TODO -- ELK 
# -> remove plTransit section from index 
# -> remove els field from.fromUnsure
# -> add document thumbnail using IIIF

import xml.etree.cElementTree as ET
from datetime import datetime
import json
import datetime
import time
import sys

from modules.elastic_funct import *
from modules.utils_funct import *
from modules.geo_functs import *

### FILES SECTION
# xml dataset
# xmlFilename='test.xml'
xmlFilename='xml_corpus_new_syntax.xml'

### ELASTICSEARCHSECTION
# Index name (can be changed according to casestudy) 
# Set here ES INDEX name
avvisi_euronews_esindex='euronews_avvisi'
# Json filename with all es documents inserted (can be changed according to casestudy) - to be used for debug purposes
jsonFilename='avvisi.json'

# Index mapping
avvisi_euronews_esmapping ={
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
         "miaDocURL":{
            "type":"text"
         },
         "hub":{
            "properties":{
               "date":{
                  "type":"date",
                  "format":"dd/MM/yyyy"
               },
               "dateUnsure":{
                  "type":"boolean"
               },
               "location":{
                  "type":"geo_point"
               },
               "placeName":{
                  "type":"text"
               },
               "placeUnsure":{
                  "type":"boolean"
               }
            }
         },
         "from":{
            "properties":{
               "date":{
                  "type":"date",
                  "format":"dd/MM/yyyy"
               },
               "dateUnsure":{
                  "type":"boolean"
               },
               "location":{
                  "type":"geo_point"
               },
               "placeName":{
                  "type":"text"
               },
               "placeUnsure":{
                  "type":"boolean"
               }
            }
         },
         "plTransit":{
            "properties":{
               "date":{
                  "type":"date",
                  "format":"dd/MM/yyyy"
               },
               "dateUnsure":{
                  "type":"boolean"
               },
               "location":{
                  "type":"geo_point"
               },
               "placeName":{
                  "type":"text"
               },
               "placeUnsure":{
                  "type":"boolean"
               }
            }
         },
         "transcription":{
            "type":"text", 
            "fielddata": True
         },
         "transcription_keyword":{
            "type":"keyword"
         },
         "topics":{
            "type":"text", 
            "fielddata": True
         },
         "topics_keyword":{
            "type":"keyword"
         },
         "newsPosition":{
            "type":"text"
         }
      }
   }
}

### Declaring global variables (these will be removed - TODO)
miaUrlPrefix="https://mia.medici.org/Mia/index.html#/mia/document-entity/"
miaUrlIIIFPrefix="https://mia.medici.org/Mia/json/iiif/getIIIFImage/"
miaUrlPostfix="/full/150,/0/default.jpg"
repository=collection=volume=""
hubPlaceLat=hubPlaceLon=hubPlaceName=""
fromPlaceLat=fromPlaceLon=fromPlaceName=""
plTransitDate=plTransitName=plTransitLat=plTransitLon=""
locationDict = {}

### FUNCTIONS
def documentPutElasticsearch(documentId, miaDocId, newsId, repository, collection, volume, hubPlaceName, hubDate, hubPlaceLat, hubPlaceLon, newsPosition, fromPlaceName, fromPlaceLat, fromPlaceLon, fromUnsure, fromDate, fromDateUnsure, topics, transcription):
    esDoc = {
        "newsId":newsId,
        "miaDocId":miaDocId,
        "miaDocURL":miaUrlPrefix+miaDocId,
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
            "dateUnsure": fromDateUnsure,
            "location": {
                "lat": fromPlaceLat,
                "lon": fromPlaceLon
            },
            "placeName":fromPlaceName,
            "fromUnsure": fromUnsure,

        },
        "plTransit":{
            #To be changed
            "date": plTransitDate,
            "location": {
                "lat": plTransitLat,
                "lon": plTransitLon
            },
            "placeName":plTransitName

        },
        "transcription": transcription,
        "transcription_keyword": transcription,
        "topics": topics,
        "topics_keyword": topics,
        "newsPosition":newsPosition
    }

    # Check for exceptions and take necessary actions (removes json fields which are not necessary since they are empty)        
    if fromUnsure is None:
        del esDoc['from']['fromUnsure']
    
    if fromDateUnsure is None:
        del esDoc['from']['dateUnsure']

    # these controls has to be rewritten
    if hubPlaceLat is not None:
        try:
            float(hubPlaceLat)
            if hubPlaceLon is not None:
                try:
                    float(hubPlaceLon)
                    pass
                except ValueError:
                    del esDoc['hub']['location']
            else:
                del esDoc['hub']['location']
        except ValueError:
            del esDoc['hub']['location']
    else:
        del esDoc['hub']['location']

    if fromPlaceLat is not None:
        try:
            float(fromPlaceLat)
            if fromPlaceLon is not None:
                try:
                    float(fromPlaceLon)
                    pass
                except ValueError:
                    del esDoc['from']['location']
            else:
                del esDoc['from']['location']
        except ValueError:
            del esDoc['from']['location']
    else:
        del esDoc['from']['location']

    if plTransitLat is not None:
        try:
            float(plTransitLat)
            if plTransitLon is not None:
                try:
                    float(plTransitLon)
                    pass
                except ValueError:
                    del esDoc['plTransit']['location']
            else:
                del esDoc['plTransit']['location']
        except ValueError:
            del esDoc['plTransit']['location']
    else:
        del esDoc['plsTransit']['location']

    if plTransitDate is not None:
        try:
            if not plTransitDate:
                del esDoc['plTransit']['date']
        except ValueError:
            del esDoc['plTransit']['date']
    else:
        del esDoc['plTransit']['date']
    
    # if plTransitName is not None:
    #     try:
    #         if not plTransitName:
    #             del esDoc['plTransit']['name']
    #     except ValueError:
    #         del esDoc['plTransit']['name']
    # else:
    #     del esDoc['plTransit']['name']
    
    #print(str(esDoc))

    res = es.index(index=avvisi_euronews_esindex, id=documentId, body=esDoc)
    print(str(res))

# Writes a Json file for Elasticsearch (just for test -- this could contain encoding problems) # TODO this has to be rewritten does not work
def writeJson(esDoc, documentId, miaDocId, newsPosition, transcription):
    newsId=miaDocId+"-"+newsPosition
    # Define Json Structure
    if esDoc.get(transcription) is not None:
        print("PRIMA " + esDoc.get(transcription))

    print(str(esDoc))
    # writing JSON object to file
    with open(jsonFilename, 'a') as f:
        f.write('\n')
        f.write('PUT '+avvisi_euronews_esindex+'/_doc/'+str(documentId))
        json.dump(esDoc, f, indent=4, ensure_ascii=False)
        f.close()


### -- MAIN --
def populateElasticsearchIndex():
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
            #print(hubDate)
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
                datetime(int(year),int(month),int(day))
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
            #Get Hub PLACES
            print('--start---')
            for x in header.iter('hub'):
                hubPlaceName=x.text
                if hubPlaceName is None:
                    pass
                    print('DocId: '+ miaDocId +'Hub name is not present - fix this')
                    sys.exit()
                else:
                    # fix place hubs - Germania, Hungary becomes Germania
                    hubPlaceName, sep, tail = hubPlaceName.partition(',') # takes only the first place (removes all places after ,) # --need to rewiew this
                    geocoord=getGeoCoordinates(hubPlaceName)
                    latitude=geocoord[0]
                    longitude=geocoord[1]
                    hubPlaceLat=latitude
                    hubPlaceLon=longitude
            
            ### NewsFrom section ###
                newsfrom = header.find('newsFrom')
                if newsfrom is None:
                    ## Documents without news -- only newsHeader is present --- TO BE FIXED --- IMPORTANT -- DOES NOT PUT 
                    print('Document [' + miaDocId+'] - document without news - NOT PUT')
                else:
                    # Documents with news
                    for newsFrom in header.iter('newsFrom'):
                        # Get News Position
                        newsPosition=(newsFrom.find('position'))
                        if newsPosition is None:
                            newsPosition="1"
                        else:
                            newsPosition=existstr(newsFrom.find('position').text)
                        # Get newsId
                        newsId=miaDocId+"-"+newsPosition
                        
                        # Get Topics
                        newsTopicList=[]
                        for x in newsFrom.iter('newsTopic'):
                            newsTopicTag=x
                            if newsTopicTag or newsTopicTag.text is None:
                                print('Document [' + miaDocId+'-'+newsPosition+'] - <newsTopic> tag is missing or without value') # TODO - POPULATE?
                                pass
                            else:
                                newsTopicList.append(newsTopicTag.text)
                        topics=newsTopicList

                        # Get FromPLaces 
                        for x in newsFrom.iter('from'):
                            fromPlaceTag=x
                            if fromPlaceTag or fromPlaceTag.text is None:
                                print('Document [' + miaDocId+'-'+newsPosition+'] - <from> tag is missing or without value') # TODO - POPULATE?
                                pass
                            else:
                                fromPlaceName=x.text
                                geocoord=getGeoCoordinates(fromPlaceName)
                                latitude=geocoord[0]
                                longitude=geocoord[1]
                                fromPlaceLat=latitude
                                fromPlaceLon=longitude
                            
                                # fromUnsure attribute
                                fromUnsure=fromPlaceTag.get('fromUnsure')
                                if fromUnsure is None: # or fromUnsure !='y' (this is tricky since the use could have put the wrong value here)
                                    pass
                                else:
                                    fromUnsure="true"

                                ##Get Date value for place
                                fromDate=fromPlaceTag.get('date')

                                if fromDate is None:
                                    fromDate=hubDate
                                  
                                # Check date format
                                fromDate=check_date_format(fromDate, miaDocId, newsPosition)

                                # dateUnsure attribute (newsFrom)
                                fromDateUnsure=fromPlaceTag.get('dateUnsure')
                                if fromDateUnsure is None: # or fromDateUnsure !='y' (this is tricky since the use could have put the wrong value here)
                                    pass
                                else:
                                    fromDateUnsure="true"

                                # Get Trascription Values
                                transcriptionTag=newsFrom.find('transc')
                                if transcriptionTag or transcriptionTag.text is None:
                                    print('Document [' + miaDocId+'-'+newsPosition+'] - <transcription> tag is missing or without value') # TODO - POPULATE?
                                    transcription=""
                                    pass
                                else:
                                    transcription=newsFrom.find('transc').text    
                                


                                plTransitDate=plTransitName=plTransitLat=plTransitLon=""

                                # Push ES document into esindex
                                #print(documentId, newsId, esindex, hubPlaceName, hubDate, newsPosition, fromPlaceName, fromUnsure, fromDate, fromDateUnsure, transcription)
                                documentPutElasticsearch(documentId, miaDocId, newsId, repository, collection, volume, hubPlaceName, hubDate, hubPlaceLat, hubPlaceLon, newsPosition, fromPlaceName, fromPlaceLat, fromPlaceLon, fromUnsure, fromDate, fromDateUnsure, topics, transcription)
                                #input('asd')
                                #writeJson() #TODO
                                print('--end---')
                                documentId=documentId+1
        #input('asd')                          
                            
def main():
    print('EURONEWS PROJECT')
    # Inizialize ES index (delete previous + create a new one) 
    createIndexElasticsearch(avvisi_euronews_esindex, avvisi_euronews_esmapping)
    time.sleep(1)

    print('Populating index with name: ' + avvisi_euronews_esindex)
    populateElasticsearchIndex()

    
if __name__ == "__main__":
    main()