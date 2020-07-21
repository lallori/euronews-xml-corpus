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

# xmlFilename='testXmlGeo.xml'
xmlFilename='xml-corpus-geo.xml'
jsonFilename='prova.json'
esindex='p2p'

documentId=int()
hubDate=str()
hubPlaceLat=str()
hubPlaceLon=str()
hubPlaceName=str()
fromDate=str()
fromPlaceLat=str()
fromPlaceLon=str()
fromPlaceName=str()

def getXmlValues():
    global documentId,hubDate,hubPlaceLat,hubPlaceLon,hubPlaceName,fromDate,fromPlaceLat,fromPlaceLon,fromPlaceName
    tree = ET.parse(xmlFilename)
    root = tree.getroot()
    documentId=1
    for header in root.iter('newsHeader'):
        #Get Date Hub Values
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
        #Get Hub Values
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
        for newsFrom in header.iter('newsFrom'):
            #Get Date Hub Values
            #---> check if fromDate is none.
            i=0
            fromDate=newsFrom.find('date').text
            if fromDate is None:
                fromDate="01/01/0001"
            else:
                fromDate=fromDate.strip()
                if ' ' in fromDate:
                    fromDate.rstrip()
                    fromDate = fromDate.split(' ', 1)[0]
                    print(fromDate)
                if ';' in fromDate:
                    fromDate = fromDate.replace(";", " ")
                    fromDate = fromDate.split(' ', 1)[0]
                    print(fromDate)
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
                # Date invalid - putting default value - this has to be changed 
                fromDate="01/01/0001" 
            # Get From Values
            for x in newsFrom.iter('from'):
                fromPlaceName=x.find('placeName').text
                fromLocation=x.find('location')
                if fromLocation is None:
                    pass
                else:
                    fromPlaceLat=fromLocation.get('lat')
                    fromPlaceLon=fromLocation.get('lon')
                    print("From:  " +fromDate+":"+fromPlaceName)
                i=i+1
                #writeJson()
                documentPutElasticsearch()
                print('--end---')
                documentId=documentId+1
                print(documentId)
        


def documentPutElasticsearch():
    global documentId, esindex
    es = Elasticsearch()
    # Define Json Structure
    doc = {
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

        }
    }
    res = es.index(index=esindex, id=documentId, body=doc)
    print(res['result'])

def writeJson():
    global documentId
    # Define Json Structure
    obj = {
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

        }
    }
    # writing JSON object to file
    with open(jsonFilename, 'a') as f:
        f.write('\n')
        f.write('PUT '+esindex+'/_doc/'+str(documentId))
        json.dump(obj, f, indent=4)

getXmlValues()