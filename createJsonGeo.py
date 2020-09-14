#!/usr/bin/python3.6
# Authors Lorenzo Allori <lorenzo.allori@gmail.con>
# Script that creates pointToPoint index for Elasticsearch
# 0.1 - alpha version
# check mapping in kibana/mappingp2p.txt
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

# xmlFilename='prova.xml'
xmlFilename='xml-corpus-geo.xml'
jsonFilename='prova.json'
# esindex='euronewsp2p'
esindex='prova2'
documentId=int()

miaDocId=""
# newsId is mapDocId+position
newsId=""
hubDate=str()
hubPlaceLat=str()
hubPlaceLon=str()
hubPlaceName=str()
hubTranscription=""
fromDate=str()
fromPlaceLat=str()
fromPlaceLon=str()
fromPlaceName=str()
transcription=""
newsPosition=""

def existstr(s):
    return '' if s is None else str(s)

def filldate(d):
    d='/'.join(x.zfill(2) for x in d.split('/'))

def getXmlValues():
    global documentId,miaDocId,hubDate,hubTranscription,hubPlaceLat,hubPlaceLon,hubPlaceName,newsId,fromDate,fromPlaceLat,fromPlaceLon,fromPlaceName,transcription,newsPosition
    tree = ET.parse(xmlFilename)
    root = tree.getroot()
    documentId=1
    ### MIA doc ID ###
    for document in root.iter('newsDocument'):
        miaDocId=document.find('docid').text
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
                        # Get From PLACES
                        for x in newsFrom.iter('from'):
                            fromPlaceName=x.find('placeName').text
                            if fromPlaceName is None:
                                fromPlaceName = hubPlaceName

                            fromLocation=x.find('location')
                            if fromLocation is None:
                                fromLocation = hubLocation
                                fromPlaceLat = hubPlaceLat
                                fromPlaceLon = hubPlaceLon
                            else:
                                fromPlaceLat=fromLocation.get('lat')
                                fromPlaceLon=fromLocation.get('lon')
                                print("From:  " +fromDate+":"+fromPlaceName)
                            i=i+1
                        # Get Trascription Values
                        transcription=existstr(newsFrom.find('transc').text)
                        print("TRANSCRIPTION: " + transcription)
                        # Get Position Value
                        newsPosition=(newsFrom.find('position'))
                        if newsPosition is None:
                            newsPosition="1"
                        else:
                            newsPosition=existstr(newsFrom.find('position').text)
                        # writeJson()
                        documentPutElasticsearch()
                        print('--end---')
                        documentId=documentId+1
                        print(documentId)                  

def documentPutElasticsearch():
    global documentId, esindex, newsPosition, transcription
    es = Elasticsearch()
    # Define Json Structure
    newsId=miaDocId+"-"+newsPosition
    print(newsId + miaDocId +  fromDate + fromPlaceLat + fromPlaceLon + fromPlaceName)
    doc = {
        "newsId":newsId,
        "miaDocId":miaDocId,
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
        "transcription": transcription,
        "transcriptionk": transcription,
        "newsPosition":newsPosition
    }
    res = es.index(index=esindex, id=documentId, body=doc)
    print(res['result'])
    transcription=""

def writeJson():
    global documentId, newsPosition, transcription
    newsId=miaDocId+"-"+newsPosition
    # Define Json Structure
    obj = {
        "newsId":newsId,
        "miaDocId":miaDocId,
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
        "transcription": transcription,
        "transcriptionk": transcription,
        "newsPosition":newsPosition
    }
    if obj.get(transcription) is not None:
        print("PRIMA " + obj.get(transcription))

    # writing JSON object to file
    with open(jsonFilename, 'a') as f:
        f.write('\n')
        f.write('PUT '+esindex+'/_doc/'+str(documentId))
        json.dump(obj, f, indent=4, ensure_ascii=False)

getXmlValues()