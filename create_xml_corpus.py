#!/usr/bin/python3
# Authors Lorenzo Allori <lorenzo.allori@gmail.con>, Wouter Kreuze <kreuzewp@gmail.com>
# ver. 1.1
# TODO 
# - addurls to documents
# - add IIIF to documents
# - clean files and remove globals

# File operations
import os
import sys
import time

# Mysql section
import mysql.connector
from mysql.connector import Error

# XML Section
import xml.etree.cElementTree as ET
from lxml import etree
from io import StringIO
import xml.dom.minidom
import re
import operator

# JSON Section
import json
import jsbeautifier

# Others Section
from modules.geo_functs import * 
from modules.configParser_functs import *
from modules.mysql_funct import *
from modules.utils_funct import *

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
# Documents list to Exclude from queries (transcriptions should be reordered to create xml -- check function put_transcription_in_order() )
cannot_fix_docs="50730,51110,51904,52079,52372,52530,53126" #not finished
all_docs_to_reorder_transcr="8196,9944,27231,27250,27131,27154,27155,27230,27263,27288,27408,28432,50431,50447,50449,50450,50454,50730,50863,50879,51110,51452,51495,51610,51611,51629,51742,51750,52065,52079,52644,52908,51984,52079,52372,52530,52908"
docs_to_reorder_transcr_1575="9944,27231,27250,27154,27230,27263,27288,51452,51495,51610,51611,51629,51742,51750,52644,52908,53210"
documents_to_process_separately=all_docs_to_reorder_transcr
# Set this to 1 if you want to include the above documents
putExcludedDocs=0

# Test query do not use
qGet1600="select documentEntityId, transcription from tblDocTranscriptions where documentEntityId in (select documentEntityId from tblDocumentEnts where category='News' and (docYear='1600' or docModernYear = '1600') and flgLogicalDelete = 0 and documentEntityId not in ("+all_docs_to_reorder_transcr+")) order by documentEntityId,uploadedFileId"

# Query for All news
qGetAllNews="select documentEntityId, transcription from tblDocTranscriptions where documentEntityId in (select documentEntityId from tblDocumentEnts where category='News' and flgLogicalDelete = 0 and documentEntityId not in ("+all_docs_to_reorder_transcr+")) order by documentEntityId,uploadedFileId"
# Use this not to include excluded documents to be finished
qGetAllNewsExcl="select documentEntityId, transcription from tblDocTranscriptions where documentEntityId in (select documentEntityId from tblDocumentEnts where category='News' and flgLogicalDelete = 0 and documentEntityId not in ("+all_docs_to_reorder_transcr+","+cannot_fix_docs+")) order by documentEntityId,uploadedFileId"
print(qGetAllNewsExcl)
input('asd')
# Query to Get Avvisi only
qGetAvvisi="select documentEntityId, transcription from tblDocTranscriptions where documentEntityId in (select documentEntityId from tblDocumentEnts where typology='Avviso' and flgLogicalDelete = 0 and documentEntityId not in ("+all_docs_to_reorder_transcr+")) order by documentEntityId,uploadedFileId"

# Query to Get News-other (to check condition typology)
qGetNewOther="select documentEntityId, transcription from tblDocTranscriptions where documentEntityId in (select documentEntityId from tblDocumentEnts where category='News Other' and flgLogicalDelete = 0 and documentEntityId not in ("+all_docs_to_reorder_transcr+")) order by documentEntityId,uploadedFileId"

qGetNewsWithTimeSpan="select documentEntityId, transcription from tblDocTranscriptions where documentEntityId in (select documentEntityId from tblDocumentEnts where category='News' and (docYear='1575' or docModernYear = '1575') and flgLogicalDelete = 0 and documentEntityId not in ("+all_docs_to_reorder_transcr+")) order by documentEntityId,uploadedFileId"

qGetDocuments=qGetAllNewsExcl
# qGetDocuments=qGet1600


# Files involved
# configFile is defined in modules/configParser_functs.py
filename="xml_corpus.xml"
filenametmp="xml_tmp.xml"
filename_test='xml_corpus_test.xml'
filenamewithids="xml_corpus_with_ids.xml"
newCorpusFile = "newCorpusFile.xml"
# locationDictFile is defined in modules/geo_functs.py

# Get configfile properties
myhost,myport,myuser, mypasswd, mydbname=read_configfile()

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
        mydb = db_connection()
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

def put_transcription_in_order(docId):
    docId=str(docId)
    try:
        mydb = db_connection()
        mycursor = mydb.cursor() 
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

    sortedFolios=sorted(folios, key=operator.itemgetter(0, 1))

    orderedTransc=""
    for record in sortedFolios:
        orderedTransc=orderedTransc+record[3]

    # clean scren 
    print(chr(27) + "[2J")
    # processing count
    print('--> Re-ordering transcriptions in documents')
    print('Processing document: ' + docId)
    return orderedTransc

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
    global mydb, cannot_fix_docs
    with open(filenametmp, 'w') as f:
        current_time=currentDateAndTime()
        # Header content of the xml file
        f.write('<?xml version="1.0"?>\n')
        f.write('<news xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"\n')
        f.write('\txsi:noNamespaceSchemaLocation="news.xsd">\n\n')
        f.write('<xmlCorpusDate>' + current_time[0] + '</xmlCorpusDate>\n')
        f.write('<xmlCorpusTime>' + current_time[1] + '</xmlCorpusTime>\n')
           
        try:
            mydb = db_connection()
            mycursor = mydb.cursor()
            mycursor.execute(qGetDocuments)

            myresult = mycursor.fetchall()

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

            # Creating and launch query to get all the transcriptions which is actually all the XML 
            qGetTranscriptions="select documentEntityId, transcription from tblDocTranscriptions where documentEntityId in {}".format(docsWithXMLT)
            mycursor.execute(qGetTranscriptions)
            myresult = mycursor.fetchall()

            if includeNoXMLDocs is True:
                for x in myresult:
                    # put each entry in file 
                    f.write('<docid>'+str(x[0])+'</docid>\n')
                    # (URI to be added here)
                    # 
                    f.write('\t\t'+x[1]+'\n')
                    
                
                 # Include excluded docs (not ordered transcr)
                if putExcludedDocs == 1:
                    docs_transcr_to_be_ordered = documents_to_process_separately.split (",")
                    # remove "cannot fix" documents
                    cannot_fix_docs=cannot_fix_docs.split(",")
                    for x in cannot_fix_docs:
                        try: 
                            docs_transcr_to_be_ordered.remove(x)
                        except ValueError:
                            print("Cannot fix document: "+x+" is not in list")
                            pass
            
                    for docId in docs_transcr_to_be_ordered:
                        f.write('<docid>'+str(docId)+'</docid>\n')
                        xmltrascr=put_transcription_in_order(docId)
                        f.write('\t\t'+xmltrascr+'\n')
                    f.write('</news>\n')
                else: 
                    f.write('</news>\n')

            else:
                for x in myresult:
                    # put each entry in file (URI to be added)
                    f.write('<docid>'+str(x[0])+'</docid>\n')
                    # Put Document MIA URI here
                    #f.write('<miaDocumentUri>https://mia.medici.org/Mia/index.html#/mia/document-entity/'+str(x[0])+'</miaDocumentUri>')
                    f.write('\t\t'+x[1]+'\n')
                
                # Include excluded docs (not ordered transcr)
                if putExcludedDocs == 1:
                    docs_transcr_to_be_ordered = documents_to_process_separately.split (",")
                    # remove "cannot fix" documents
                    cannot_fix_docs=cannot_fix_docs.split(",")
                    for x in cannot_fix_docs:
                        try: 
                            docs_transcr_to_be_ordered.remove(x)
                        except ValueError:
                            print("Cannot fix document: "+x+" is not in list")
                            pass

                    for docId in docs_transcr_to_be_ordered:
                        f.write('<docid>'+str(docId)+'</docid>\n')
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
            # remove empty newsDocuments
            xml_to_check=xml_to_check.replace('<newsDocument>\n</newsDocument>','')

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
    
    # Remove empty newsDocument tags
    prettyxml=prettyxml.replace('<newsDocument>\n</newsDocument>','')

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
            
            mydb = db_connection()
            mycursor1 = mydb.cursor()
            mycursor1.execute(qGetUploaded)
            myresult1 = mycursor1.fetchall()
            uploadFileId=str(myresult1[0][0])
            
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
            print('--> Getting Archival Properties for each document --- documents processed: ' + str(count))
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
                # print("From"  +longitude)

                if latitude and longitude:
                    locationTag = ET.Element('location', dict(lon=longitude, lat=latitude))
                    x.append(locationTag)
        else:
            pass
        count+=1
        # clean scren 
        print(chr(27) + "[2J")
        # processing count
        print('--> Adding places: ' + str(count) + 'please wait...')

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
        print(chr(27) + "[2J")
        # processing count
        print('--> Place of Transit places: ' + str(count) + 'please wait...')
    
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
    print('--> Connection to DB ok!')
    print('')
    print('--> Creating Rough XML')
    create_rough_xml()
    print('')
    print('--> Rough XML ok!')
    ##check_formatted_xml()
    fix_NewsDocument()
    # input('break')
    fix_newsHeader()
    print('')
    print('--> News Document and News Header ok!')
    print('')
    if includeNoXMLDocs is True:
        createXmlForDocsWithoutXml()
        print('--> Included documents without XML ok!')
        print('')
    print('')
    add_ArchivalProperties()
    finalizeXmlCorpus()
    check_formatted_xml()
    print('--> XML Corpus finalized ok!!')
    print('')
    print('Adding geo coordinates... this could take time!')
    addGeoCoordToXml()
    
    # clean screen before giving final results.
    print(chr(27) + "[2J")
    print('--XML Corpus Ready!--')
    print('')
    print('Total documents added:'+totalDocumentsCount)
    print('')
    print('DONE')
    ##finalizeXmlCorpus()

if __name__ == "__main__":
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))









