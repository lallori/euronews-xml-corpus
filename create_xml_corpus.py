#!/usr/bin/python3.6
# Authors Lorenzo Allori <lorenzo.allori@gmail.con>, Wouter Kreuze <kreuzewp@gmail.com>
# ver. 1.0
# Todo - addurls to documents.

# File operations
import os
import sys
from configparser import SafeConfigParser
import time

# Mysql section
import mysql.connector
from mysql.connector import Error

# XML Section
import xml.etree.cElementTree as ET
from lxml import etree
from io import StringIO
import xml.dom.minidom
from datetime import datetime
import re
import operator


# JSON Section
import json
import jsbeautifier

# Others Section
from geopy.geocoders import Nominatim


# Script parameters
if len(sys.argv) != 2: 
    print("Usage: python3 euronewsPrj_create_xml_corpus.py (xmlonly|all)")
    exit() 

par = str(sys.argv[1])

if par == 'all':
    includeNoXMLDocs = True
elif par == 'xmlonly':
    includeNoXMLDocs = False
else:
    print("Usage: python3 euronewsPrj_create_xml_corpus.py (xmlonly|all)")
    exit()

# SQL Queries
# Documents list to Exclude from queries
documentListToExclude="8196,9944,27231,27250,27131,27154,27155,27230,27263,27288,50431,50447,50449,50450,50454,50863,50879,51110,51452,51495,51610,51611,51629,51742,51750,52065,52079"
putExcludedDocs="1"

# Test query do not use
qGet1600="select documentEntityId, transcription from tblDocTranscriptions where documentEntityId in (select documentEntityId from tblDocumentEnts where category='News' and (docYear='1600' or docModernYear = '1600') and flgLogicalDelete = 0 and documentEntityId not in ("+documentListToExclude+")) order by documentEntityId,uploadedFileId"

# Query for All news
qGetAllNews="select documentEntityId, transcription from tblDocTranscriptions where documentEntityId in (select documentEntityId from tblDocumentEnts where category='News' and flgLogicalDelete = 0 and documentEntityId not in ("+documentListToExclude+")) order by documentEntityId,uploadedFileId"

# Query to Get Avvisi only
qGetAvvisi="select documentEntityId, transcription from tblDocTranscriptions where documentEntityId in (select documentEntityId from tblDocumentEnts where typology='Avviso' and flgLogicalDelete = 0 and documentEntityId not in ("+documentListToExclude+")) order by documentEntityId,uploadedFileId"

# Query to Get News-other (to check condition typology)
qGetNewOther="select documentEntityId, transcription from tblDocTranscriptions where documentEntityId in (select documentEntityId from tblDocumentEnts where category='News Other' and flgLogicalDelete = 0 and documentEntityId not in ("+documentListToExclude+")) order by documentEntityId,uploadedFileId"

qGetDocuments=qGetAvvisi
# qGetDocuments=qGet1600


# Files involved
configFile="configFile.ini"

filename="xml_corpus.xml"
filenametmp="xml_tmp.xml"
filename_test='xml_corpus_test.xml'
filenamewithids="xml_corpus_with_ids.xml"
newCorpusFile = "newCorpusFile.xml"
locationDictFile = "locationDict.txt"


# Get config properties
host=str()
port=str()
user=str()
passwd=str()
database=str()

if os.path.isfile(configFile):
    parser = SafeConfigParser()
    parser.read(configFile)
    myhost=parser.get('MYSQLDATABASE', 'mysql.host')
    myport=parser.get('MYSQLDATABASE', 'mysql.port')
    myuser=parser.get('MYSQLDATABASE', 'mysql.user')
    mypasswd=parser.get('MYSQLDATABASE', 'mysql.passwd')
    mydbname=parser.get('MYSQLDATABASE', 'mysql.dbname')
else:
    print("Config file not found")

# Mysql Connection properties
mydb = mysql.connector.connect(
        host=myhost,
        port=myport,
        user=myuser,
        passwd=mypasswd,
        database=mydbname
        )

# Setting other globals
prettyxml = ""
city = ""
locationDict= {}
comma=','
count=int()
transcription=""
docDay=""
docMonth=""
docYear=""
docModernYear=""
placeOfOrigin=""
latitude=""
longitude=""
docsWithXML=list()
docsMaybeWithoutXML=list()
docsWithoutXML=list()
totalDocumentsCount=""

# Get Date and Time
def currentDateAndTime():
    now = datetime.now()
    current_date = now.strftime("%Y-%m-%d")
    current_time = now.strftime("%H:%M:%S")
    return current_date, current_time
    # print(current_date + ' '  + current_time)

# Test Database connection
def test_connection(): 
    try:
        global mydb
        if mydb.is_connected():
            db_Info = mydb.get_server_info()
            print("Connected to MySQL Server version ", db_Info)
            cursor = mydb.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            print("You're connected to database: ", record)
    except Error as e:
        print("Error while connecting to MySQL", e)
###### TEST FUNCTIONS


###### PROD FUNCTIONS
def placeNameStrip(placeName):
    if(placeName is None or placeName == ""):
        print('no placename')
        pass
    else:
        placeName=placeName.replace(" ", "")
        print(placeName)
        placeName=placeName.split(', ')
        print(placeName)
    for x in placeName:
        print(x)

# Get docFields if document trascription does not contain xml
def getDocFields(docId):
    qGetTransc="select transcription from tblDocTranscriptions where documentEntityId="
    qGetDocFields="select  docDay, docMonth, docYear, docModernYear, placeOfOrigin from tblDocumentEnts where documentEntityId="
    qGetPlaceOfOrigin="select PLACENAME from tblPlaces where PLACEALLID="
    global transcription
    global docDay
    global docMonth
    global docYear
    global docModernYear
    global placeOfOrigin

    try:
        mydb = mysql.connector.connect(
        host=myhost,
        port=myport,
        user=myuser,
        passwd=mypasswd,
        database=mydbname
        )
        mycursor = mydb.cursor()        

        # Get Basic Doc Fields from tblDocumentEnts
        docId=str(docId)
        getDocFields=qGetDocFields+docId
        mycursor.execute(getDocFields)

        results = mycursor.fetchall()
        for row in results:
            docDay=row[0]
            docMonth=row[1]
            docYear=row[2]
            docModernYear=row[3]
            placeOfOrigin=str(row[4])
            if docModernYear !=0:
                docYear=docModernYear
            # print(str(docDay) + "/" + str(docMonth) + "/" +  str(docYear))
            # Get Place of Compilation (if exist)
            # print(placeOfOrigin)
            if placeOfOrigin == "None":
                    pass
            else:
                getPlaceOfOrigin=qGetPlaceOfOrigin+placeOfOrigin
                mycursor.execute(getPlaceOfOrigin)
                results = mycursor.fetchall()
                # print(getPlaceOfOrigin)
                for row in results:
                    placeOfOrigin=row[0]

            getTranscription=qGetTransc+docId
            mycursor.execute(getTranscription)
            results = mycursor.fetchall()
            for row in results:
                transcription=row[0]
        # have to remove globals (TODOS)
        return transcription, docDay, docMonth, docYear, docModernYear, placeOfOrigin

    except Error as e:
        print("Error while connecting to MySQL", e)

def loadGeoLocationsFile():
    global locationDict
    if os.path.isfile('./'+locationDictFile):
        dictionary = json.load(open(locationDictFile))
        locationDict = dictionary
    else:
        locationDict = {}


def put_transcription_in_order(docId):
    docId=str(docId)
    print('docid= ' + docId)
    try:
        mydb = mysql.connector.connect(
        host=myhost,
        port=myport,
        user=myuser,
        passwd=mypasswd,
        database=mydbname
        )
        mycursor = mydb.cursor() 
        # print("Mysql connected")
        # input('break')
    except:
        print('Connection to db failed')
        sys.exit()

    getTranscription='select uploadedFileId,transcription from tblDocTranscriptions where documentEntityId='

    getTranscriptions=getTranscription+docId
    mycursor.execute(getTranscriptions)

    results = mycursor.fetchall()

    folios=[] # create list
    
    for document in results:
        uploadedFileId=document[0]
        transcription=document[1]
        # print(uploadedFileId)
        getFolio='select folioNumber,rectoverso from tblUploadFilesFolios where uploadedFileId='
        getFolio=getFolio+str(uploadedFileId)
        mycursor.execute(getFolio)
        folioprop = mycursor.fetchall()        
        folioNumber=folioprop[0][0]
        rectoverso=folioprop[0][1]
        folios.append([folioNumber,rectoverso,uploadedFileId,transcription])
        # print(str(folios))
        # TODO check for null values and noNumb=1    

    # print(str(folios))
    sortedFolios=sorted(folios, key=operator.itemgetter(0, 1))
    # print(str(sortedFolios))

    orderedTransc=""
    for record in sortedFolios:
        orderedTransc=orderedTransc+record[3]
    return orderedTransc


def getGeoCoordinates(city):
    global locationDict
    geolocator = Nominatim(user_agent='myapplication')
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
        location = geolocator.geocode(city)
        if location != None:    
            latitude=str(location.latitude)
            longitude=str(location.longitude)
            locationDict.setdefault(city, [])
            locationDict[city]['lat'] = latitude
            locationDict[city]['long'] = longitude
        else:
            latitude = ""
            longitude = ""
    return latitude, longitude

def createXmlForDocsWithoutXml():
    with open(filename, "r+", encoding = "utf-8") as f:
        f.seek(0, os.SEEK_END)
        pos = f.tell() - 1

        while pos > 0 and f.read(1) != "\n":
            pos -= 1
            f.seek(pos, os.SEEK_SET)
        if pos > 0:
            f.seek(pos, os.SEEK_SET)
            f.truncate()

        print('Processing... please wait.. this can take time...')
        count=0
        for x in docsWithoutXML:
            getDocFields(x)
            f.write('<newsDocument>\n')
            f.write('<docid>'+str(x)+'</docid>\n')
            f.write('<newsHeader>\n')
            if placeOfOrigin == "None":
                pass
            else:
                f.write('<hub>'+ placeOfOrigin +'</hub>\n')
            f.write('<date>'+str(docDay) + "/" + str(docMonth) + "/" +  str(docYear)+'</date>\n')
            f.write('<newsFrom>\n')
            f.write('<from>'+ placeOfOrigin +'</from>\n')
            f.write('<date>'+str(docDay) + "/" + str(docMonth) + "/" +  str(docYear)+'</date>\n')
            f.write('<transc>'+transcription+'</transc>\n')
            f.write('<position>1</position>\n')
            f.write('</newsFrom>\n')
            f.write('</newsHeader>\n')
            f.write('</newsDocument>\n')
            count+=1
            # clean scren 
            print(chr(27) + "[2J")
            # processing count
            print('Documents without XML processed: ' + str(count))
        f.write('</news>')

# Write corpus file
def create_rough_xml():
    global mydb
    with open(filenametmp, 'w') as f:
        current_time=currentDateAndTime()
        # Header content of the xml file
        def printheader():
            f.write('<?xml version="1.0"?>\n')
            
            f.write('<news xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"\n')
            f.write('\txsi:noNamespaceSchemaLocation="news.xsd">\n\n')

            # f.write('<news>\n')
            f.write('<xmlCorpusDate>' + current_time[0] + '</xmlCorpusDate>\n')
            f.write('<xmlCorpusTime>' + current_time[1] + '</xmlCorpusTime>\n')
           
        try:
            mydb
            mycursor = mydb.cursor()
            mycursor.execute(qGetDocuments)

            myresult = mycursor.fetchall()

            printheader()

            # define list with documents which do not contain XML
            global docsWithXML            
            global docsMaybeWithoutXML
            global docsWithoutXML
            allDocs=list()
            
            for x in myresult:
                allDocs.append(x[0])

            for x in myresult:
                #print(x)
                if x[1] is not None:
                    if '>' in x[1]: 
                        # put each entry in file (URI to be added)
                        docsWithXML.append(x[0])
                    else:
                        docsMaybeWithoutXML.append(x[0])
            docsWithoutXML=[x for x in docsMaybeWithoutXML if x not in docsWithXML]
            
            # Converting into tuples
            docsWithXMLT=tuple(docsWithXML)
            qGetTranscriptions="select documentEntityId, transcription from tblDocTranscriptions where documentEntityId in {}".format(docsWithXMLT)
            
            mydb
            mycursor = mydb.cursor()
            mycursor.execute(qGetTranscriptions)
            myresult = mycursor.fetchall()

            if includeNoXMLDocs is True:
                for x in myresult:
                    # put each entry in file (URI to be added)
                    f.write('<docid>'+str(x[0])+'</docid>\n')
                    f.write('\t\t'+x[1]+'\n')
                f.write('</newsDocument>\n')

                 # Include excluded docs (not ordered transcr)
                if putExcludedDocs == 1:
                    docs_transcr_to_be_ordered = documentListToExclude.split (",")

                    for docId in docs_transcr_to_be_ordered:
                        xmltrascr=put_transcription_in_order(docId)
                        f.write('\t\t'+xmltrascr+'\n')
                    f.write('</news>\n')
                else: 
                    f.write('</news>\n')

            else:
                for x in myresult:
                    # put each entry in file (URI to be added)
                    f.write('<docid>'+str(x[0])+'</docid>\n')
                    f.write('\t\t'+x[1]+'\n')

                # Include excluded docs (not ordered transcr)
                if putExcludedDocs == 1:
                    docs_transcr_to_be_ordered = documentListToExclude.split (",")

                    for docId in docs_transcr_to_be_ordered:
                        xmltrascr=put_transcription_in_order(docId)
                        f.write('\t\t'+xmltrascr+'\n')
                    f.write('</news>\n')
                else:
                    f.write('</news>\n') 

            # Print totals
            # print(len(allDocs))
            # print(len(docsWithXML))
            # print(len(docsWithoutXML))

        except Error as e:
            print("Error reading data from MySQL table", e)

    f.closed

    # Remove carriage returns
    cmd = 'sed -e "s/\r//g" ' + filenametmp + ' > ' + filename
    cmd2 = 'rm ' + filenametmp
    os.system(cmd)
    os.system(cmd2)

# Check and Formatting corpus file
def check_formatted_xml():
    with open(filename, 'r') as f:
        xml_to_check = f.read()

        try:
            etree.parse(StringIO(xml_to_check))
            print('XML well formed, syntax ok.')

        # check for file IO error
        except IOError:
            print('Invalid File')

        # check for XML syntax errors
        except etree.XMLSyntaxError as err:
            print('XML Syntax Error, see error_syntax.log')
            with open('error_syntax.log', 'w') as error_log_file:
                error_log_file.write(str(err.error_log))
            quit()

        except:
            print('Unknown error, exiting.')
            quit()
    f.closed

# Format indented xml
def prettyXml():
    global prettyxml
    dom = xml.dom.minidom.parse(filename)
    
    prettyxml = dom.toprettyxml()

    lines = prettyxml.split("\n")
    non_empty_lines = [line for line in lines if line.strip() != ""]
    
    prettyxml = ""

    for line in non_empty_lines:
        prettyxml += line + "\n"


# Fix newsHeader field
def fix_newsHeader():
    global prettyxml
    
    prettyXml()

    try:
        reSubIn = r'(</newsHeader>)((.|\s)+?)(</newsDocument>|<newsHeader)'
        reSubOut = r'\2</newsHeader>\4'

        prettyxml = re.sub(reSubIn, reSubOut, prettyxml)

    except:
        Error
    
    with open(filename, 'w') as f:
        f.write(prettyxml)
    f.closed

def fix_NewsDocument():
    global prettyxml
    newsDocument = '</newsDocument>\n<newsDocument>'

    with open(filename, 'r') as f:
        prettyxml = f.read()

    for docId in re.findall(r'(?:<docid>)(\d+)', prettyxml):
        prettyxml = prettyxml.replace('<docid>'+docId+'</docid>', newsDocument+'<docid id="'+docId+'">'+'</docid>', 1)
        prettyxml = prettyxml.replace('<docid>'+docId+'</docid>', '')
        prettyxml = prettyxml.replace('<docid id="'+docId+'">'+'</docid>','<docid>'+docId+'</docid>')
            
    prettyxml = prettyxml.replace('<newsDocument></newsDocument>', '')
    prettyxml = prettyxml.replace('</newsDocument>', '', 1)

    if includeNoXMLDocs is True:
        pass
    else:
        prettyxml = re.sub('</news>$','</newsDocument>\n</news>',prettyxml)

    with open(filename, 'w') as f:
        f.write(prettyxml)

def add_ArchivalProperties():
    global prettyxml, mydb, totalDocumentsCount

    # Getting repositories dictionary
    repoDict = json.load(open("repositoriesDict.txt"))

    print('- Adding Archival Properties to XML- please wait...')

    with open(filename, 'r') as f:
        prettyxml = f.read()
        count=0
        for docId in re.findall(r'(?:<docid>)(\d+)', prettyxml):
            qGetUploaded="select uploadedFileId from tblDocTranscriptions where documentEntityId ="+docId
            
            mydb
            mycursor1 = mydb.cursor()
            mycursor1.execute(qGetUploaded)
            myresult1 = mycursor1.fetchall()
            uploadFileId=str(myresult1[0][0])
            # print(uploadFileId)
            
            qGetRepoColl="select repository, collectionName from tblCollections where id in (select collection from tblUploads where id in (select idTblUpload from tblUploadedFiles where id ='" + uploadFileId +"'))"
            mycursor1.execute(qGetRepoColl)
            myresult1 = mycursor1.fetchall()
            repository=str(myresult1[0][0])
            # getting repository name from repositoriesDict.txt generated from tblRepositories
            repository=(repoDict["1"])
            collection=str(myresult1[0][1])

            qGetVolume="select volume from tblVolumes where SUMMARYID in (select volume from tblUploads where id in (select idTblUpload from tblUploadedFiles where id ='" + uploadFileId +"'))"
            mycursor1.execute(qGetVolume)
            myresult1 = mycursor1.fetchall()
            volume=str(myresult1[0][0])

            prettyxml = prettyxml.replace('<docid>'+docId+'</docid>', '<docid>'+docId+'</docid><repository>'+repository+'</repository><collection>'+collection+'</collection><volume>'+volume+'</volume>', 1)
            count+=1
            # clean scren 
            # print("\b" * len(message), end="", flush=True)
            print(chr(27) + "[2J")
            # processing count
            print('Documents processed: ' + str(count))
            totalDocumentsCount=str(count)
        
    with open(filename, 'w') as f:
        f.write(prettyxml)



# Output final xmlcorpus
def finalizeXmlCorpus():
    prettyXml()
    
    with open(filename, 'w') as f:
        f.write(prettyxml)
    f.closed
    
    if (mydb.is_connected()):
        mydb.close()
        mydb.close()
        print("MySQL connection is closed")

# add geographical coordinates to xml corpus
def addGeoCoordToXml():
    loadGeoLocationsFile()
    print('- Adding Geographical Locations to XML- please wait...')
    tree = ET.parse(filename)
    root = tree.getroot()

    count=0
    for x in root.iter('hub'):
        # print(x.text)
        city = x.text
        # Removing hub/from/plTransit value
        x.text=''
        # Adding new placename tag with value
        placeNameTag=ET.Element('placeName')
        x.append(placeNameTag)
        placeNameTag.text = city
        # Adding new locationTag with values
        if city != None:
            if city != 'na' and city != 'NA' and city !='Na' and city !='[loss]' and city !='[unsure]' and city !='xxx':
                if comma in city:
                    city = city.replace(comma, " ")
                    city = city.split(' ', 1)[0]
                else:
                    pass     
                geocoord=getGeoCoordinates(city)
                latitude=geocoord[0]
                longitude=geocoord[1]
                print(longitude)


                if latitude and longitude:
                    locationTag = ET.Element('location', dict(lon=longitude, lat=latitude))
                    x.append(locationTag)
        else:
            pass
        count+=1
        # clean scren 
        # print(chr(27) + "[2J")
        # processing count
        print('Hubs places: ' + str(count))

    count=0
    for x in root.iter('from'):
        print("qui")
        # print(x.text)
        city = x.text
        # Removing hub/from/plTransit value
        x.text=''
        # Adding new placename tag with value
        placeNameTag=ET.Element('placeName')
        x.append(placeNameTag)
        placeNameTag.text = city
        # Adding new locationTag with values
        if city != None:
            if city != 'na' and city != 'NA' and city !='Na' and city !='[loss]' and city !='[unsure]' and city !='xxx':
                if comma in city:
                    city = city.replace(comma, " ")
                    city = city.split(' ', 1)[0]
                else:
                    pass     
                geocoord=getGeoCoordinates(city)
                latitude=geocoord[0]
                longitude=geocoord[1]
                print("From"  +longitude)

                if latitude and longitude:
                    locationTag = ET.Element('location', dict(lon=longitude, lat=latitude))
                    x.append(locationTag)
        else:
            pass
        count+=1
        # clean scren 
        # print(chr(27) + "[2J")
        # processing count
        print('News places: ' + str(count))

    count=0
    for x in root.iter('plTransit'):
        # print(x.text)
        city = x.text
        # Removing hub/from/plTransit value
        x.text=''
        # Adding new placename tag with value
        placeNameTag=ET.Element('placeName')
        x.append(placeNameTag)
        placeNameTag.text = city
        # Adding new locationTag with values
        if city != None:
            if city != 'na' and city != 'NA' and city !='Na' and city !='[loss]' and city !='[unsure]' and city !='xxx':
                if comma in city:
                    city = city.replace(comma, " ")
                    city = city.split(' ', 1)[0]
                else:
                    pass     
                geocoord=getGeoCoordinates(city)
                latitude=geocoord[0]
                longitude=geocoord[1]

                if latitude and longitude:
                    locationTag = ET.Element('location', dict(lon=longitude, lat=latitude))
                    x.append(locationTag)
        else:
            pass
        count+=1
        # clean scren 
        # print(chr(27) + "[2J")
        # processing count
        print('Place of Transit places: ' + str(count))
    
    # Update location dictionary file
    with open(locationDictFile, 'w') as f:
        f.write(json.dumps(locationDict))
        f.close()

    # Parse it
    formattedJson = jsbeautifier.beautify_file(locationDictFile)
    with open(locationDictFile, 'w') as f:
        f.write(formattedJson)
        f.close()

    tree.write('xml-corpus-geo.xml', encoding="UTF-8")

#### RUN
def main():
    test_connection()
    print('Connection to DB ok!')
    print('')
    print('Loading..')
    create_rough_xml()
    print('- Rough XML ok!')
    ##check_formatted_xml()
    fix_NewsDocument()
    fix_newsHeader()
    print('')
    print('- News Document and News Header ok!')
    print('')
    if includeNoXMLDocs is True:
        createXmlForDocsWithoutXml()
        print('- Included documents without XML ok!')
        print('')
    print('')
    add_ArchivalProperties()
    finalizeXmlCorpus()
    check_formatted_xml()
    print('- XML Corpus finalized ok!!')
    print('')
    print('Adding geo coordinates... this could take time!')
    addGeoCoordToXml()
    print('- Added geo coordinates attributes! - check xml-corpus-geo.xml')
    print('')
    print('Total documents added:'+totalDocumentsCount)
    print('')
    print('DONE')
    ##finalizeXmlCorpus()

if __name__ == "__main__":
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))









