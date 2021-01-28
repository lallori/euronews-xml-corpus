#!/usr/bin/python3
# Fuctions to access ElasticSearch data
# Author Lorenzo Allori <lorenzo.allori@gmail.com>
import time
from elasticsearch import Elasticsearch
from modules.configParser_functs import *

# Get elasticsearch properties
eshost,esport,esuser,espassword = elastic_properties()

# Es authentication (default is 1)
esauth=1
if esauth==0:
    es = Elasticsearch(['http://'+eshost+':'+esport], http_auth=(esuser, espassword), timeout=120)
    print(es)
    input('asd')
else:
    es = Elasticsearch()

# Set here ES INDEX name
esindex='avvisi'
# Json filename with all es documents inserted (can be changed according to casestudy) - to be used for debug purposes
jsonFilename='avvisi.json'

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
         "transcriptionk":{
            "type":"keyword"
         },
         "newsPosition":{
            "type":"text"
         }
      }
   }
}


def createIndexElasticsearch():
   # Deleting previous Index
    res = es.indices.delete(index=esindex, ignore=[400,404])
    print('Deleting ES Index '+esindex+' response:'+str(res))
    
    # Creating new Index
    res = es.indices.create(index=esindex, body=mapping, ignore=400)
    print('')
    print('Creating ES Index '+esindex+' response:'+str(res))
    time.sleep(3)
