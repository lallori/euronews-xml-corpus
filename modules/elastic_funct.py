#!/usr/bin/python3
# Fuctions to access ElasticSearch data
# Author Lorenzo Allori <lorenzo.allori@gmail.com>
import time
from elasticsearch import Elasticsearch
from modules.configParser_functs import *

# Get elasticsearch properties
eshost,esport,esuser,espassword = elastic_properties()

# Es authentication (1 is without es authentication, 0 is with authentication - auth parameters are in configFile.ini)
esauth=1
if esauth==0:
    es = Elasticsearch(['http://'+eshost+':'+esport], http_auth=(esuser, espassword), timeout=120)
    print(es)
else:
    es = Elasticsearch()

def createIndexElasticsearch(esindex, mapping):
   # Deleting previous Index
    res = es.indices.delete(index=esindex, ignore=[400,404])
    print('Deleting ES Index '+esindex+' response:'+str(res))
    
    # Creating new Index
    res = es.indices.create(index=esindex, body=mapping, ignore=400)
    print('')
    print('Creating ES Index '+esindex+' response:'+str(res))
    time.sleep(3)
