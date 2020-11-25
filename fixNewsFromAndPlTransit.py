#!/usr/bin/python3.6
# Creates the xml for new <from> <plTransit> tags format. 
# TODO: 
# - add fuction to calculate approx date when not inserted or date with problems
# - add <dateUnsure> options

from io import open
from configparser import SafeConfigParser
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
import itertools

## FILES
logfile="euronewsxmlcorpus.log"
xmlcorpus="xml_corpus.xml"
# xmlcorpus="prova.xml"
new_corpusfile="prova_mod.xml"
location_dict_file = "locationDict.txt"

# define globals
location_dict = {}
dateunsuremsg = "date can be approximate"

# Get Date and Time
def currentDateAndTime():
    now = datetime.now()
    current_date = now.strftime("%Y-%m-%d")
    current_time = now.strftime("%H:%M:%S")
    return current_date, current_time

#initialize logging
logf=open(logfile, "a+")
current_date, current_time=currentDateAndTime()
logf.write(os.path.basename(__file__)+' - StartLog--> '+current_date+' '+current_time+'\n')

def existstr(s):
    return '' if s is None else str(s)

def check_place(cplace):
    excludedtermlist="NA,na,[loss],[NA],[unsure],xxx,xx,x,XXX,XX,X,xX"
    listexcl=excludedtermlist.split (",")
    if cplace in listexcl:
        cplace="[lost]"    
    else:
        pass
    return cplace

def enum_place(cplace,miaDocId,newsposition):
    if cplace is None:
        if newsposition is None:
            logf.write('SEVERE: '+ 'Document: '+ miaDocId+'<from> and <position> tags content is missing ---this could be a problem'+'\n')
            pass
        else:
            logf.write('SEVERE: '+ 'Document: '+ miaDocId+'-'+str(newsposition)+ ' <from> tag content is missing ---this could be a problem'+'\n')
        pass
        cplacel=['[not-filled]']
        placelistcount=1 
    else:
        # mangles the list of dates in a <from> tag
        cplacel=[]
        # enum the list and create list from which create the <from> tag with attribs
        cplace=cplace.strip()
        cplace=cplace.replace(";",",")
        # create list
        cplacelw=cplace.split(",")
        for x in cplacelw:
            stripx=x.strip()
            fixedplace=check_place(stripx)
            cplacel.append(fixedplace)
        
        # print(str(cplacel))

        placelistcount=len(cplacel)

    return cplacel, placelistcount
    


def check_date(cdate,ctype,parentdate,miaDocId):
    # this funct takes three variables (params)
    # (cdate: the date which has to be checked, 
    # ctype(depending on the section - headerdate, newsfromdate, pltransitdate), 
    # parentdate(the date to be used when the date is wrongly formatted))
    dateunsure=None
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
            print('No date in document or Wrong date format in HEADER: ' + "miaDocId: " + miaDocId + " Date: "  + cdate+'\n')
            sys.exit()
        elif ctype == "newsfromdate":
            dateunsure=1
            #print('No date in document or Wrong date format in NEWSFROM: ' + "miaDocId: " + miaDocId + " Date: "  + cdate)
            cdate=parentdate
            if ' ' in cdate:
                cdate = cdate.split(' ', 1)[0]
                # print(cdate)
            if ';' in cdate:
                    cdate = cdate.replace(";", " ")
                    cdate = cdate.split(' ', 1)[0]
            pass
        elif ctype == "pltransitdate":
            dateunsure=1
            cdate=parentdate
            cdate=cdate.strip()

            if ' ' in cdate:
                cdate = cdate.split(' ', 1)[0]
                # print(cdate)
            if ';' in cdate:
                    cdate = cdate.replace(";", " ")
                    cdate = cdate.split(' ', 1)[0]
            pass
        else:
            print('ERROR in date format (pls check dates in document: ' + miaDocId + ')')

    return cdate, dateunsure


def enum_date(cdate,ctype,parentdate,miaDocId):
    # mangles the list of dates in a <from> tag
    cdatel=[]
    
    # enum
    cdate=cdate.strip()
    cdate=cdate.replace(","," ")
    cdate=cdate.replace(";"," ")
    cdate=" ".join(cdate.split())

    # cdatelw=cdate.split(" ")
    # for x in cdatelw:
    #     # fixedate, dateunsure=check_date(x,ctype,parentdate,miaDocId)
    #     # cdatel.append(fixedate)

    cdatel=cdate.split(" ")
    # print(str(cdatel))
    datelistcount=len(cdatel)

    return cdatel, datelistcount


def print_pretty_xml():
    # Hack function to get a pretty printed xml without encoding problems
    # formatting xml
    dom=xml.dom.minidom.parse(new_corpusfile)
    pretty_xml_text=dom.toprettyxml()
    
    # removing blank lines
    lines = pretty_xml_text.split("\n")
    non_empty_lines = [line for line in lines if line.strip() != ""]
    pretty_xml_text = ""
    for line in non_empty_lines:
        pretty_xml_text += line + "\n"

    # write file
    pretty_xml_file=open(new_corpusfile, "w")
    pretty_xml_file.write(pretty_xml_text)

def create_xml_for_newsfrom(logf):
    # this function creates the actual xml to be used as dataset for creating documents 
    # in Elasticsearch for the news from visualization 
    # it does fix the old method of inserting <from> and <date> -- multiple entries
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
                hubdate, hubdateunsure=check_date(hubdate,"headerdate",'01/01/0001',miaDocId)
                
                if hubdate is None:
                    print('ERROR: No Hub date in document: ' + miaDocId)
                    exit
                else:
                    # parentdate default at 01/01/0001 will never be really used
                    hubdate, hubdateunsure=check_date(hubdate,"headerdate",'01/01/0001',miaDocId)

            # enters newsFrom section
            for newsfrom in header.iter('newsFrom'):
                # get the news position in document 
                newsposition=newsfrom.find('position').text    
                if newsposition is None:
                    newsposition="1"

                # print(str(placeUnsureTag) + str(dateUnsureTag))

                ###########################placeUnsureTag=
                #<from> and <date> section#
                ###########################
                newsfromplace=newsfrom.find('from')

                if newsfromplace is None:
                    # if no <from> tag is present
                    print('WARNING: '+ 'Document: '+ miaDocId+'-'+newsposition+ ' <from> tag is missing ---this could be a problem'+'\n')
                    logf.write('WARNING: '+ 'Document: '+ miaDocId+'-'+newsposition+ ' <from> tag is missing ---this could be a problem'+'\n')
                    pass
                else:
                    newsfromplacenamelist=newsfrom.find('from').text
                    newsfromplacenamelist, fromplacelistlenght=enum_place(newsfromplacenamelist,miaDocId,newsposition)
        
                    print('asdasdsd')
                    newsfromdate=newsfrom.find('date')
                    if newsfromdate is None:
                        newsfromdate=hubdate
                    else:
                        newsfromdate=newsfrom.find('date').text
                        if newsfromdate is None:
                            newsfromdate=hubdate
                        else:
                            # Join the two lists
                            newsfromdatelist, datelistlenght=enum_date(newsfromdate,"newsfromdate",hubdate,miaDocId)
                    
                    print('--> '+miaDocId+'-'+newsposition)
                    if (datelistlenght != fromplacelistlenght):
                        print('ERROR: dates and places lists have different lenght--DOCUMENT: ' + miaDocId+'-'+newsposition+ ' ---this could be a problem'+'\n')
                        logf.write('ERROR: dates and places lists have different lenght--DOCUMENT: ' + miaDocId+'-'+newsposition+ ' ---this could be a problem'+'\n')
                    

                    dateandplacelist=list(itertools.zip_longest(newsfromplacenamelist, newsfromdatelist, fillvalue=None))
                    
                    fromtagr=newsfrom.find('from')
                    datetagr=newsfrom.find('date')

                    if fromtagr is None:
                        pass 
                    else:
                        newsfrom.remove(fromtagr)
                    
                    if datetagr is None:
                        pass 
                    else:
                        newsfrom.remove(datetagr)

                    # check if <placeUnsure> and/or <dateUnsure> are present and if they are remove them
                    placeunsuretag=newsfrom.find('fromUnsure')
                    dateunsuretag=newsfrom.find('dateUnsure')
                    
                    if placeunsuretag is None:
                        pass 
                    else:
                        newsfrom.remove(placeunsuretag)
                    
                    if dateunsuretag is None:
                        pass
                    else:
                        newsfrom.remove(dateunsuretag)

                    # Start processing the joined list
                    for x in dateandplacelist:       
                        fromtag= ET.Element('from')
                        fromtag.text=x[0]
                        newsfrom.insert(0,fromtag)
                        # Check for <fromUnsure> and fix it
                        if placeunsuretag is None:
                            pass
                        else:
                            fromtag.set('fromunsure','y')
                        # Check for <dateUnsure> and fix it
                        if dateunsuretag is None:    
                            if x[1] is None:
                                # TODO: this has to change since if there is no newfrom date it takes hubdate
                                fromtag.set('date', hubdate)
                                fromtag.set('dateunsure','y')
                            else:
                                fromdate, newsfromdateunsure=check_date(x[1],"newsfromdate",hubdate,miaDocId)
                                if newsfromdateunsure is None:
                                    # Set a default date to be used for placeoftransit when there is no date.
                                    newsfromdate_def=fromdate
                                    # Add attribute date="$date" to <from> tag
                                    fromtag.set('date', fromdate)
                                else:
                                    fromtag.set('date', fromdate)
                                    fromtag.set('dateunsure','y')
                        else:
                            if x[1] is None:
                                # TODO: this has to change since if there is no newfrom date it takes hubdate
                                fromtag.set('date', hubdate)
                                fromtag.set('dateunsure','y')
                            else:
                                fromdate, newsfromdateunsure=check_date(x[1],"newsfromdate",hubdate,miaDocId)
                                if newsfromdateunsure is None:
                                    # Set a default date to be used for placeoftransit when there is no date.
                                    newsfromdate_def=fromdate
                                    # Add attribute date="$date" to <from> tag
                                    fromtag.set('date', fromdate)
                                    fromtag.set('dateunsure','y')
                                else:
                                    fromtag.set('date', fromdate)
                                    fromtag.set('dateunsure','y')

            
                #########################################
                #<plTransit> and <plTransitDate> section#
                #########################################
                pltransitplace=newsfrom.find('plTransit')

                if pltransitplace is None:
                    # if no <plTransit> tag is present
                    pass
                else:
                    # Initialize list lenght
                    pltransitplacelist=[]
                    pltransitplacelistlenght=0
                    # Populate place list
                    pltransitplacelist=newsfrom.find('plTransit').text
                    pltransitplacelist, pltransitplacelistlenght=enum_place(pltransitplacelist,miaDocId,newsposition)

                    # Initialize list lenght
                    pltransitdatelist=[]
                    pltransitdatelistlenght=0
                    # Populate date list
                    pltransitdate=newsfrom.find('plTransitDate')
                    if pltransitdate is None:
                        pltransitdate=hubdate
                    else:
                        pltransitdate=newsfrom.find('plTransitDate').text
                        if pltransitdate is None:
                            pltransitdate=hubdate
                        else:
                            pltransitdatelist, pltransitdatelistlenght=enum_date(pltransitdate,"pltransitdate",hubdate,miaDocId)

                    if (pltransitdatelistlenght != pltransitplacelistlenght):
                        print('ERROR: PLTRANSIT dates and places lists have different lenght--DOCUMENT: ' + miaDocId+'-'+newsposition+ ' ---this could be a problem'+'\n')
                        logf.write('ERROR: PLTRANSIT dates and places lists have different lenght--DOCUMENT: ' + miaDocId+'-'+newsposition+ ' ---this could be a problem'+'\n')
                    
                    pltransitdateandplacelist=list(itertools.zip_longest(pltransitplacelist, pltransitdatelist, fillvalue=None))

                    pltransittagr=newsfrom.find('plTransit')
                    pltransitdatetagr=newsfrom.find('plTransitDate')
                    newsfrom.remove(pltransittagr)
                    if pltransitdatetagr is None:
                        pass
                    else:
                        newsfrom.remove(pltransitdatetagr)

                    for x in pltransitdateandplacelist:       
                        pltransittag= ET.Element('plTransit')
                        pltransittag.text=x[0]
                        newsfrom.insert(-3,pltransittag)
                        # if no date for pltransit
                        if x[1] is None:
                            # TODO: this has to change since if there is no placeoftransit date it takes newsfromdate
                            pltransittag.set('date', newsfromdate_def)
                            pltransittag.set('dateunsure','y')
                        else:
                            pltransitdate, pltransitdateunsure=check_date(x[1],"pltransitdate",fromdate,miaDocId)
                            if pltransitdateunsure is None:
                                pltransittag.set('date', pltransitdate)
                            else:
                                pltransittag.set('date', pltransitdate)
                                pltransittag.set('dateunsure','y')
                    

                    # print(str(pltransitplacelist))
                    # print(str(pltransitdatelist))
                    # print(str(pltransitdateandplacelist))

                # print('break')           
    tree.write(new_corpusfile, encoding="UTF-8")
    print_pretty_xml()

def main():
    create_xml_for_newsfrom(logf)
    logf.close()


if __name__ == "__main__":
    main()