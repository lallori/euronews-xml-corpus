#!/usr/bin/python3.6
# Creates the xml to catch the placeOfTransit
# Creates the mapping on Elasticsearch
# TODO: add a field which specify the hub so that we can filter using the hubname

import xml.etree.cElementTree as ET
from datetime import datetime
import json
import datetime
import time
import sys

from modules.elastic_funct import *
from modules.utils_funct import *
from modules.geo_functs import *

# xmlcorpus="xml_corpus.xml"
xmlcorpus="test.xml"
# xmlcorpus="xml_corpus_new_syntax.xml"

newstransitxmlcorpusfile="test.xml"
location_dict_file = "locationDict.txt"

# Elasticsearch section
esindexname='euronews_pltransit'
esauth=0
if esauth==1:
    es = Elasticsearch(['http://localhost:9200'], http_auth=(esuser, espassword))
else:
    es = Elasticsearch()


# define globals
location_dict = {}
dateunsuremsg = "date can be approximate"


def check_date_trans(cdate,ctype,parentdate):
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
                "hub":{
                    "properties":{
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
    stage ,transcription,ifhub):

    # Define Json Structure
    print(newsId)
    # esDoc mapping is defined in the variables section above
    
    # if ifhub=="1":
    #     esDoc = {
    #         "newsId":newsId,
    #         "source":{
    #             "date": srcPlTransitDate,
    #             "date-unsure": srcDateUnsure,
    #             "date-notes": srcDateNotes,
    #             "location": {
    #                 "lat": srcPlTransitLat,
    #                 "lon": srcPlTransitLon
    #             },
    #             "placeName":srcPlTransitName
    #         },
    #         "destination":{
    #             "date": destPlTransitDate,
    #             "date-unsure": destDateUnsure,
    #             "date-notes": destDateNotes,
    #             "location": {
    #                 "lat": destPlTransitLat,
    #                 "lon": destPlTransitLon
    #             },
    #             "placeName":destPlTransitName

    #         },
    #         "hub":{
    #             "location": {
    #                 "lat": destPlTransitLat,
    #                 "lon": destPlTransitLon
    #             },
    #             "placeName":destPlTransitName

    #         },
    #         "stage": stage,
    #         "transcription": transcription,
    #     }
    # else:
    #     #if ifhub is 0 do not add hub section (it happens when the istance is a middletransit place)
    #     esDoc = {
    #         "newsId":newsId,
    #         "source":{
    #             "date": srcPlTransitDate,
    #             "date-unsure": srcDateUnsure,
    #             "date-notes": srcDateNotes,
    #             "location": {
    #                 "lat": srcPlTransitLat,
    #                 "lon": srcPlTransitLon
    #             },
    #             "placeName":srcPlTransitName
    #         },
    #         "destination":{
    #             "date": destPlTransitDate,
    #             "date-unsure": destDateUnsure,
    #             "date-notes": destDateNotes,
    #             "location": {
    #                 "lat": destPlTransitLat,
    #                 "lon": destPlTransitLon
    #             },
    #             "placeName":destPlTransitName
    #         },
    #         "stage": stage,
    #         "transcription": transcription,
    #     }
    
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
        "hub":{
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
    # input('break')
    
    res=es.index(index=esindexname, body=esDoc) 
    print(str(res))

def populate_es_index():
    # Loading geo coordinates dictionary is already created
    load_geo_location_file()

    # Parse the place f transit XML dataset
    tree = ET.parse(xmlcorpus)
    root = tree.getroot()

    # Start querying the dataset
    for document in root.iter('newsDocument'):
        miaDocId=document.find('docid').text
         ### Header section ###
        for header in document.iter('newsHeader'):
            # Get Hub Name
            try:
                hubfrom=check_place(header.find('hub').text)
            except:
                print('ERROR: No Hub Name in document: ' + miaDocId)
                sys.exit()
            # Get Hub date
            hubdate=header.find('date')
            # print(miaDocId)
            if hubdate is None:
                print('ERROR: No Hub date in document: ' + miaDocId)
                sys.exit()
            else:
                hubdate=header.find('date').text
                hubdate=check_date_trans(hubdate,"headerdate",'01/01/0001')
                
                if hubdate is None:
                    print('ERROR: No Hub date in document: ' + miaDocId)
                    exit
                else:
                    # parentdate default at 01/01/0001 will never be really used
                    check_date_trans(hubdate,"headerdate",'01/01/0001')
            
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
                    print('ERROR - news position field is not present in document: ' + miaDocId)
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
                        newsfromdate=check_date_trans(newsfromdate,"newsfromdate",hubdate)

                # Create place of transit list
                placeoftransitlist = newsfrom.findall('plTransit')
                
                if len(placeoftransitlist) == 0:
                    print('No place of transit in this document: ' + newsId) # not inserting in map--- are we sure?? #TODO
                    pass
                else:
                    # ENDPOINT - Source Section
                    lastplaceoftransit = placeoftransitlist[-1] # get last item of list
                    ifhub="1"
                    try:
                        srcPlTransitDate=lastplaceoftransit.attrib['date']
                    except:
                        srcPlTransitDate=newsfrom.find('date')
                    
                    try:   
                        stage=lastplaceoftransit.attrib['stage']
                    except:
                        stage=""

                    if 'dateUnsure' in lastplaceoftransit.attrib:
                        srcDateUnsure=True
                        if srcDateUnsure:
                            srcDateNotes=dateunsuremsg #do we want this? #TODO
                    else:
                        srcDateUnsure=""
                        srcDateNotes=""
                    try:
                        srcPlTransitName=check_place(lastplaceoftransit.text)
                    except:
                        try:
                            srcPlTransitName=check_place(newsfrom.find('from').text)
                        except:
                            srcPlTransitName=""
                            print('problems----pltransit empty??' + newsId)
                            sys.exit()

                    coordinates=get_geo_coordinates(srcPlTransitName)
                    srcPlTransitLat=coordinates[0]
                    srcPlTransitLon=coordinates[1]

                    # ENDPOINT - Destination Section - (Obviously the newshub)
                    try:
                        destPlTransitName=check_place(header.find('hub').text)
                    except:
                       print('Error in creating ENDPOINT, newsId: ' + newsId)
                       input('break - want to continue?')
                    #    sys.exit()

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
                            destDateUnsure=True   
                            destDateNotes=dateunsuremsg                     
                    except:
                        destDateUnsure=""
                        destDateNotes=""

                    # Crating ES Document for ENDPOINT
                    try:
                        put_document_es_index(esindexname,newsId,srcPlTransitDate,srcDateUnsure,\
                                            srcDateNotes,srcPlTransitLat,srcPlTransitLon,srcPlTransitName,destPlTransitDate,\
                                            destDateUnsure,destDateNotes,destPlTransitLat,destPlTransitLon,destPlTransitName,\
                                            stage ,transcription, ifhub)
                    except:
                        print('Error in creating ENDPOINT, newsId: ' + newsId)
                        input('break - want to continue?')

                    del srcPlTransitDate,srcDateUnsure,\
                        srcDateNotes,srcPlTransitLat,srcPlTransitLon,srcPlTransitName,destPlTransitDate,\
                        destDateUnsure,destDateNotes,destPlTransitLat,destPlTransitLon,destPlTransitName,\
                        stage

                    
                    # # Create MIDDLE TRANSIT network TODO
                    for findplaceoftransit in placeoftransitlist:
                        position=placeoftransitlist.index(findplaceoftransit)
                        if findplaceoftransit != placeoftransitlist[-1]:
                            ifhub="0" # this is a middle transit not a hub (when creating the esdocument it does not populate the hub section)
                            # Source
                            try:
                                srcPlTransitDate=findplaceoftransit.attrib['date']
                            except:
                                srcPlTransitDate=newsfrom.find('date')
                            
                            try:   
                                stage=findplaceoftransit.attrib['stage']
                            except:
                                stage=""

                            if 'unsure' in findplaceoftransit.attrib:
                                srcDateUnsure=True
                                if srcDateUnsure:
                                    srcDateNotes=dateunsuremsg
                            else:
                                srcDateUnsure=""
                                srcDateNotes=""
                            try:
                                srcPlTransitName=check_place(findplaceoftransit.text)
                            except:
                                try:
                                    srcPlTransitName=check_place(newsfrom.find('from').text)
                                except:
                                    srcPlTransitName

                            coordinates=get_geo_coordinates(srcPlTransitName)
                            srcPlTransitLat=coordinates[0]
                            srcPlTransitLon=coordinates[1]
                            # Destination
                            nextposition=position+1
                            nextplaceoftransit=placeoftransitlist[nextposition]
                            print()
                            try:
                                destPlTransitDate=nextplaceoftransit.attrib['date']
                            except:
                                destPlTransitDate=newsfrom.find('date')
                            
                            try:   
                                stage=nextplaceoftransit.attrib['stage']
                            except:
                                stage=""

                            if 'unsure' in nextplaceoftransit.attrib:
                                destDateUnsure=True
                                if destDateUnsure:
                                    destDateNotes=dateunsuremsg
                            else:
                                destDateUnsure=""
                                destDateNotes=""
                            try:
                                destPlTransitName=check_place(nextplaceoftransit.text)
                            except:
                                try:
                                    destPlTransitName=check_place(newsfrom.find('from').text)
                                except:
                                    destPlTransitName

                            coordinates=get_geo_coordinates(destPlTransitName)
                            destPlTransitLat=coordinates[0]
                            destPlTransitLon=coordinates[1]

                            # Crating ES Document for Middle Transit
                            try:
                                put_document_es_index(esindexname,newsId,srcPlTransitDate,srcDateUnsure,\
                                                    srcDateNotes,srcPlTransitLat,srcPlTransitLon,srcPlTransitName,destPlTransitDate,\
                                                    destDateUnsure,destDateNotes,destPlTransitLat,destPlTransitLon,destPlTransitName,\
                                                    stage ,transcription,ifhub)
                            except:
                                print('Error in creating MIDDLESTAGE, newsId: ' + newsId + '  ' + nextplaceoftransit.text)
                                input('break - want to continue?')

                            del srcPlTransitDate,srcDateUnsure,\
                                srcDateNotes,srcPlTransitLat,srcPlTransitLon,srcPlTransitName,destPlTransitDate,\
                                destDateUnsure,destDateNotes,destPlTransitLat,destPlTransitLon,destPlTransitName,\
                                stage
                            
                        else:
                            pass



def main():
    create_es_index()     # Inizialize ES index - removes the old on, create a new one
    populate_es_index()

if __name__ == "__main__":
    main()