#!/usr/bin/python3.6
# Creates the xml to catch the placeOfTransit

from io import open
import xml.etree.cElementTree as ET
from lxml import etree
from io import StringIO
import xml.dom.minidom
from datetime import datetime
import sys
from types import SimpleNamespace
import json
from elasticsearch import Elasticsearch
import uuid
import unicodedata
from geopy.geocoders import Nominatim
import os.path
import jsbeautifier
import time

# xmlcorpus="xml_corpus.xml"
xmlcorpus="prova.xml"
newstransitxmlcorpusfile="xml-corpus-plTransit.xml"
location_dict_file = "locationDict.txt"

# define globals
esindexname='pltransit'
location_dict = {}
dateunsuremsg = "date can be approximate"

def existstr(s):
    return '' if s is None else str(s)

def check_place(cplace):
    comma=","
    if cplace != None:
        if cplace != 'na' and cplace != 'NA' and cplace !='Na' and cplace !='[loss]' and cplace !='[unsure]' and cplace !='xxx':
            if comma in cplace:
                cplace = cplace.replace(comma, " ")
                cplace = cplace.split(' ', 1)[0]
            else:
                pass     
    else:
        pass
    return cplace


def check_date(cdate,ctype,parentdate):
    # this funct takes three variables (params)
    # (cdate: the date which has to be checked, 
    # ctype(depending on the section - headerdate, newsfromdate, pltransitdate), 
    # parentdate(the date to be used when the date is wrongly formatted))
    cdate=cdate.strip()
    if ' ' in cdate:
        cdate = cdate.split(' ', 1)[0]
        # print(cdate)
    if ';' in cdate:
            cdate = cdate.replace(";", " ")
            cdate = cdate.split(' ', 1)[0]
            # print(cdate)
    #fill with zeros the date if wrong format (ex. 6/6/1534 becomes 06/06/1534)
    cdate='/'.join(x.zfill(2) for x in cdate.split('/'))

    try:
        #check if date is correctly formatted
        datetime.strptime(cdate, '%d/%m/%Y')    
    except ValueError:
        if ctype == "headerdate":
            print('No date in document or Wrong date format in HEADER: ' + "miaDocId: " + miaDocId + " Date: "  + cdate)
            sys.exit()
        elif ctype == "newsfromdate":
            #print('No date in document or Wrong date format in NEWSFROM: ' + "miaDocId: " + miaDocId + " Date: "  + cdate)
            cdate=parentdate
            pass
        elif ctype == "pltransitdate":
            cdate=parentdate
            pass
        else:
            print('ERROR in date format (pls check dates in document: ' + miaDocId + ')')
    return cdate

def create_xml_for_place_of_transit():
    # this function creates the actual xml to be used as dataset for creating documents 
    # in Elasticsearch for the newstransit visualization 
    # it does fix the old method of inserting plTransit and plTransitDate
    tree = ET.parse(xmlcorpus)
    root = tree.getroot()

    for document in root.iter('newsDocument'):
        for header in document.iter('newsHeader'):
            # enters newsHeader section
            miaDocId=document.find('docid').text
            hubdate=header.find('date')
            # print(miaDocId)
            if hubdate is None:
                print('ERROR: No Hub date in document: ' + miaDocId)
                sys.exit()
            else:
                hubdate=header.find('date').text
                hubdate=check_date(hubdate,"headerdate",'01/01/0001')
                
                if hubdate is None:
                    print('ERROR: No Hub date in document: ' + miaDocId)
                    exit
                else:
                    # parentdate default at 01/01/0001 will never be really used
                    check_date(hubdate,"headerdate",'01/01/0001')
            # enters newsFrom section
            for newsfrom in header.iter('newsFrom'):
                newsfromdate=newsfrom.find('date')
                if newsfromdate is None:
                    newsfromdate=hubdate
                else:
                    newsfromdate=newsfrom.find('date').text
                    if newsfromdate is None:
                        newsfromdate=hubdate
                    else:
                        newsfromdate=check_date(newsfromdate,"newsfromdate",hubdate)
        
                for findPlaceTransit in newsfrom.findall('plTransit'):    
                    if findPlaceTransit is None:
                        pass
                    else:
                        pltransitdate = newsfrom.findall('plTransitDate')
                        if len(pltransitdate) == 0:
                            pltransitdate=newsfrom.find('date')
                            if pltransitdate is None:
                                pltransitdate = newsfromdate
                            else:
                                pltransitdate = newsfrom.find('date').text
                                pltransitdate=check_date(pltransitdate,"pltransitdate",newsfromdate)
                            findPlaceTransit.set('date',pltransitdate)
                            findPlaceTransit.set('dateunsure','yes')
                            findPlaceTransit.set('stage','1')
                        else:
                            for pltransitdate in newsfrom.findall('plTransitDate'):
                                pltransitdatevalue=pltransitdate.text
                                pltransitdatevalue=check_date(pltransitdatevalue,"pltransitdate",newsfromdate)
                                findPlaceTransit.set('date',pltransitdatevalue)
                                findPlaceTransit.set('stage','1')
                                newsfrom.remove(pltransitdate)
    tree.write(newstransitxmlcorpusfile, encoding="UTF-8")

# -- GEOLOCATIONS --
def load_geo_location_file():
    global location_dict
    if os.path.isfile('./'+location_dict_file):
        dictionary = json.load(open(location_dict_file))
        location_dict = dictionary
    else:
        location_dict = {}

def get_geo_coordinates(cplace):
    global location_dict
    geolocator = Nominatim(user_agent='myapplication')
    if cplace in location_dict:
        if 'lat' in location_dict[cplace]:
            latitude=location_dict[cplace]['lat']
        else:
            latitude = ""
        if 'long' in location_dict[cplace]:
            longitude=location_dict[cplace]['long']
        else:
            longitude = ""
    else:
        location_dict[cplace] = {}
        location = geolocator.geocode(cplace)
        if location != None:    
            latitude=str(location.latitude)
            longitude=str(location.longitude)
            location_dict.setdefault(cplace, [])
            location_dict[cplace]['lat'] = latitude
            location_dict[cplace]['long'] = longitude
        else:
            latitude = ""
            longitude = ""
    return latitude, longitude



# -- ELASTICSEARCH --
def create_es_index():
    # Index mapping
    mapping ={
        "mappings":{
            "properties":{
                "newsId":{
                    "type":"text"
                },
                "source":{
                    "properties":{
                        "date":{
                            "type":"date",
                            "format":"dd/MM/yyyy"
                        },
                        "date-unsure":{
                            "type": "boolean"
                        },
                        "date-notes":{
                        "type": "text"
                        },
                        "location":{
                            "type":"geo_point"
                        },
                        "placeName":{
                            "type":"text"
                        }
                    }
                },
                "destination":{
                    "properties":{
                        "date":{
                            "type":"date",
                            "format":"dd/MM/yyyy"
                        },
                        "date-unsure":{
                            "type": "boolean"
                        },
                        "date-notes":{
                        "type": "text"
                        },
                        "location":{
                            "type":"geo_point"
                        },
                        "placeName":{
                            "type":"text"
                        }
                    }
                },
                "stage":{
                    "type":"text"
                },
                "transcription":{
                    "type":"text"
                }
            }
        }
    }

    # Deleting previous Index
    res = Elasticsearch().indices.delete(index=esindexname, ignore=[400,404])
    print('Deleting ES Index '+esindexname+' response:'+str(res))
    
    # Creating new Index
    res = Elasticsearch().indices.create(index=esindexname, body=mapping, ignore=400)
    print('')
    print('Creating ES Index '+esindexname+' response:'+str(res))
    time.sleep(3)


def put_document_es_index(esindexname,newsId,srcPlTransitDate,srcDateUnsure,\
    srcDateNotes,srcPlTransitLat,srcPlTransitLon,srcPlTransitName,destPlTransitDate,\
    destDateUnsure,destDateNotes,destPlTransitLat,destPlTransitLon,destPlTransitName,\
    stage ,transcription):
    es = Elasticsearch()
    # Define Json Structure
    print(newsId)
    # esDoc mapping is defined in the variables section above
    esDoc = {
        "newsId":newsId,
        "source":{
            "date": srcPlTransitDate,
            "date-unsure": srcDateUnsure,
            "date-notes": srcDateNotes,
            "location": {
                "lat": srcPlTransitLat,
                "lon": srcPlTransitLon
            },
            "placeName":srcPlTransitName
        },
        "destination":{
            "date": destPlTransitDate,
            "date-unsure": destDateUnsure,
            "date-notes": destDateNotes,
            "location": {
                "lat": destPlTransitLat,
                "lon": destPlTransitLon
            },
            "placeName":destPlTransitName

        },
        "stage": stage,
        "transcription": transcription,
    }

    print(str(esDoc))
    input('break')

    res = es.index(index=esindexname, body=esDoc)
    print(str(res))

def populate_es_index():
    # Inizialize ES index
    #create_es_index()

    # Loading geo coordinates dictionary is already created
    load_geo_location_file()

    # Parse the place f transit XML dataset
    tree = ET.parse(newstransitxmlcorpusfile)
    root = tree.getroot()

    # Start querying the dataset
    for document in root.iter('newsDocument'):
        miaDocId=document.find('docid').text
         ### Header section ###
        for header in document.iter('newsHeader'):
            # Get Hub Name
            hubfrom=header.find('hub').text
            
            # Get Hub date
            hubdate=header.find('date')
            # print(miaDocId)
            if hubdate is None:
                print('ERROR: No Hub date in document: ' + miaDocId)
                sys.exit()
            else:
                hubdate=header.find('date').text
                hubdate=check_date(hubdate,"headerdate",'01/01/0001')
                
                if hubdate is None:
                    print('ERROR: No Hub date in document: ' + miaDocId)
                    exit
                else:
                    # parentdate default at 01/01/0001 will never be really used
                    check_date(hubdate,"headerdate",'01/01/0001')
            # enters newsFrom section
            for newsfrom in header.iter('newsFrom'):
                # get transcription
                try: 
                    transcription=newsfrom.find('transc').text
                except:
                    transcription=""

                # get newsId
                try:
                    newsposition=newsfrom.find('position').text
                    newsId=miaDocId+"-"+newsposition
                except:
                    print('ERROR - newsprosition is not present in document: ' + miaDocId)
                    sys.exit()
                # get date
                newsfromdate=newsfrom.find('date')
                if newsfromdate is None:
                    newsfromdate=hubdate
                else:
                    newsfromdate=newsfrom.find('date').text
                    if newsfromdate is None:
                        newsfromdate=hubdate
                    else:
                        newsfromdate=check_date(newsfromdate,"newsfromdate",hubdate)

                # Create place of transit list
                placeoftransitlist = newsfrom.findall('plTransit')
                
                if len(placeoftransitlist) == 0:
                    print('vuoto')
                    pass
                else:
                    # STARTPOINT - Source Section (it is obviously newsfrom)
                    try:
                        srcPlTransitName=check_place(newsfrom.find('from').text)
                    except:
                        print('ERROR')
                        sys.exit()

                    coordinates=get_geo_coordinates(srcPlTransitName)
                    srcPlTransitLat=coordinates[0]
                    srcPlTransitLon=coordinates[1]

                    try:
                        srcPlTransitDate=newsfromdate
                    except:
                        pass

                    try: 
                        srcDateUnsure=newsfrom.find('dateUnsure').text
                        if srcDateUnsure:
                            srcDateNotes=dateunsuremsg                     
                    except:
                        srcDateUnsure=""
                        srcDateNotes=""

                    # STARTPOINT - Destination Section 
                    firstplaceoftransit = placeoftransitlist[0] # get first item of list
                    try:
                        destPlTransitDate=firstplaceoftransit.attrib['date']
                    except:
                        destPlTransitDate=newsfrom.find('date')
                    
                    # try:   
                    #     stage=firstplaceoftransit.attrib['stage']
                    # except:
                    #     stage=""
                    stage="0"

                    if 'unsure' in firstplaceoftransit.attrib:
                        destDateUnsure=True
                        if destDateUnsure:
                            destDateNotes=dateunsuremsg
                    else:
                        destDateUnsure=""
                        destDateNotes=""
                    try:
                        destPlTransitName=firstplaceoftransit.text
                    except:
                        try:
                            destPlTransitName=newsfrom.find('from').text
                        except:
                            destPlTransitName

                    coordinates=get_geo_coordinates(destPlTransitName)
                    destPlTransitLat=coordinates[0]
                    destPlTransitLon=coordinates[1]

                    # Crating ES Document for STARTPOINT
                    try:
                        put_document_es_index(esindexname,newsId,srcPlTransitDate,srcDateUnsure,\
                                            srcDateNotes,srcPlTransitLat,srcPlTransitLon,srcPlTransitName,destPlTransitDate,\
                                            destDateUnsure,destDateNotes,destPlTransitLat,destPlTransitLon,destPlTransitName,\
                                            stage ,transcription)
                    except:
                        print('Error in creating startpoing')
                        input('break - want to continue?')
                
                    del srcPlTransitDate,srcDateUnsure,\
                        srcDateNotes,srcPlTransitLat,srcPlTransitLon,srcPlTransitName,destPlTransitDate,\
                        destDateUnsure,destDateNotes,destPlTransitLat,destPlTransitLon,destPlTransitName,\
                        stage
                    
                    # ENDPOINT - Source Section
                    lastplaceoftransit = placeoftransitlist[-1] # get last item of list

                    try:
                        srcPlTransitDate=lastplaceoftransit.attrib['date']
                    except:
                        srcPlTransitDate=newsfrom.find('date')
                    
                    try:   
                        stage=lastplaceoftransit.attrib['stage']
                    except:
                        stage=""

                    if 'unsure' in lastplaceoftransit.attrib:
                        srcDateUnsure=True
                        if srcDateUnsure:
                            srcDateNotes=dateunsuremsg
                    else:
                        srcDateUnsure=""
                        srcDateNotes=""
                    try:
                        srcPlTransitName=lastplaceoftransit.text
                    except:
                        try:
                            srcPlTransitName=newsfrom.find('from').text
                        except:
                            srcPlTransitName

                    coordinates=get_geo_coordinates(srcPlTransitName)
                    srcPlTransitLat=coordinates[0]
                    srcPlTransitLon=coordinates[1]

                    # ENDPOINT - Destination Section - (Obviously the newshub)
                    try:
                        destPlTransitName=check_place(header.find('hub').text)
                    except:
                        print('ERROR')
                        sys.exit()

                    coordinates=get_geo_coordinates(destPlTransitName)
                    destPlTransitLat=coordinates[0]
                    destPlTransitLon=coordinates[1]

                    try:
                        destPlTransitDate=newsfromdate
                    except:
                        pass

                    try: 
                        destDateUnsure=header.find('dateUnsure').text
                        if destDateUnsure:
                            destDateNotes=dateunsuremsg                     
                    except:
                        destDateUnsure=""
                        destDateNotes=""

                    # Crating ES Document for STARTPOINT
                    try:
                        put_document_es_index(esindexname,newsId,srcPlTransitDate,srcDateUnsure,\
                                            srcDateNotes,srcPlTransitLat,srcPlTransitLon,srcPlTransitName,destPlTransitDate,\
                                            destDateUnsure,destDateNotes,destPlTransitLat,destPlTransitLon,destPlTransitName,\
                                            stage ,transcription)
                    except:
                        print('Error in creating startpoing')
                        input('break - want to continue?')

                    del srcPlTransitDate,srcDateUnsure,\
                        srcDateNotes,srcPlTransitLat,srcPlTransitLon,srcPlTransitName,destPlTransitDate,\
                        destDateUnsure,destDateNotes,destPlTransitLat,destPlTransitLon,destPlTransitName,\
                        stage

                    
                    # # Create MIDDLE TRANSIT network
                    for findplaceoftransit in placeoftransitlist:
                        if findplaceoftransit != placeoftransitlist[0] and findplaceoftransit != placeoftransitlist[-1]:
                            


                    input('break')


def main():
    # time.sleep(2)
    #create_xml_for_place_of_transit()
    create_es_index()
    populate_es_index()

if __name__ == "__main__":
    main()