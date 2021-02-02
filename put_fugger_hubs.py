#!/usr/bin/python3
# Authors Lorenzo Allori <lorenzo.allori@gmail.con>
# Script that creates FUGGER index for Elasticsearch 
# These index can be used to create custom visualization dashboards in Kibana
# ver 1.0
# ElasticSearch functions are in ./modules/elastic_funct.py
# Example recorda:
#Cod | folios  | hub       | date
#8951|80r-80v  | Antwerpen | 30.04.1578
#8968|514v-515r| o.O.      | NA
#
#
#
#
# TODO: web scraping for newFrom
# 1- create list in memory with all the good entries (so no date as null or hub as null)- variable: complete_fug_url
# 2- for each entry do the web scraping searching for the right values in the page.


from modules.elastic_funct import *
from modules.utils_funct import *
from modules.geo_functs import *

import csv

# Globals
logfile="fugger_log.txt"
csv_dataset="datasets/fugger.csv"
# csv_dataset="datasets/test.csv"
repository = "Österreichische Nationalbibliothek"
collection = "Sammlung für Handschriften und alte Drucke"
fuggerurl="https://fuggerzeitungen.univie.ac.at/zeitungen/cod-"

# ELASTICSEARCH
# Index name (can be changed according to casestudy) 
avvisi_fugger_esindex="avvisi_fugger"

# Index mapping
avvisi_fugger_esmapping ={
    "mappings":{
      "properties":{
        "repository":{
            "type":"keyword"
        },
        "collection":{
            "type":"keyword"
        },
        "cod":{
            "type":"keyword"
        },
        "folios":{
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
               },

            }
        },
        "url":{  # url: https://fuggerzeitungen.univie.ac.at/zeitungen/cod-8975-414r-414v
            "type":"text"
            }
      }
   }
}

def documentPutElasticsearch(documentId,repository,collection,cod,folios,date,hubLat,hubLon,hub,complete_fug_url):
    esDoc = {
        "repository":repository,
        "collection":collection,
        "cod":cod,
        "folios":folios,
        "hub":{
            "date": date,
            "location": {
                "lat": hubLat,
                "lon": hubLon
            },
            "placeName":hub
        },
        "url":complete_fug_url
    }

    res = es.index(index=avvisi_fugger_esindex, id=documentId, body=esDoc)
    print(str(res))

def populate_index():
    with open(csv_dataset) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        documentId = 1
        for row in csv_reader:
                cod=row[0]
                folios=row[1]
                hub=row[2].rstrip()
                date=row[3]
                if date !="NA": # no date no insert
                    if hub !="o.O.": #no hub no insert
                        date = date.replace('.', '/')
                        hubLat, hubLon = getGeoCoordinates(hub)
                        if hubLat == "" or hubLon == "":
                            logmessage="WARNING: ["+cod+" "+folios+"] does not have a recognizable hub --> "+hub
                            logToFile(logfile, logmessage)
                            
                            pass
                        else:
                            complete_fug_url = fuggerurl +cod+"-"+folios
                            print(cod+"|"+folios+"|"+hub+"|"+date)
                            documentPutElasticsearch(documentId,repository,collection,cod,folios,date,hubLat,hubLon,hub,complete_fug_url)
                            documentId += 1
                    else:
                        print("WARNING: ["+cod+" "+folios+"] does not have a hub so will not be inserted")
                else:
                    print("WARNING: ["+cod+" "+folios+"] does not have a date so will not be inserted")
                    pass
                line_count += 1
                
        print(f'Processed {line_count} lines.')

def main():
    createIndexElasticsearch(avvisi_fugger_esindex, avvisi_fugger_esmapping)
    populate_index()

if __name__ == "__main__":
    main()