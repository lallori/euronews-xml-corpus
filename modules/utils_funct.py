#!/usr/bin/python3
# Utility fuctions - random stuff
# Author Lorenzo Allori <lorenzo.allori@gmail.com>

from datetime import datetime
import sys

# Get Date and Time
def currentDateAndTime():
    now = datetime.now()
    current_date = now.strftime("%Y-%m-%d")
    current_time = now.strftime("%H:%M:%S")
    return current_date, current_time

def filldate(d):
    d='/'.join(x.zfill(2) for x in d.split('/'))
    return d

def existstr(s):
    return '' if s is None else str(s)

def check_date_basic(cdate):
    cdate=cdate.strip()
    if ' ' in cdate:
        cdate = cdate.split(' ', 1)[0]
        # print(cdate)
    if ';' in cdate:
            cdate = cdate.replace(";", " ")
            cdate = cdate.split(' ', 1)[0]
            # print(cdate)
    #fill with zeros the date if wrong format (ex. 6/6/1534 becomes 06/06/1534)
    #cdate='/'.join(x.zfill(2) for x in cdate.split('/'))
    cdate=filldate(cdate)
    return cdate

def check_date_format(cdate,mapDocId, position): 
    check_date_basic(cdate)
    format="%d/%m/%Y"
    try:
        datetime.strptime(cdate, format)
    except ValueError :
        print('Document: [' + mapDocId+'-'+position+'] - date is uncorrecly formatted pls check')
        sys.exit()
    return cdate


def check_date_es(cdate, mapDocId, position):# DO NOT USE - This has to be tested... (need re-writing)
    check_date_basic(cdate)
    try:
        #check if date is correctly formatted and puts into datetime format for elasticsearch DO NOT USE
        cdate=datetime.strptime(cdate, '%d/%m/%Y')
        print(cdate)
        return cdate
    except ValueError: 
        print('Document: [' + mapDocId+'-'+position+'] - date is uncorrecly formatted pls check')
        sys.exit()

def check_place(cplace):
    comma=","
    cplace=cplace.lstrip()
    cplace=cplace.rstrip()
    if cplace == 'NA':
        print('Place set to NA---> problem') # TODO log to file!
        pass
    if cplace != None:
        if cplace != 'na' and cplace != 'NA' and cplace !='Na' and cplace !='[loss]' and cplace !='[unsure]' and cplace !='xxx':
            if comma in cplace:
                #cplace = cplace.replace(comma, " ")
                cplace = cplace.split(', ', 1)[0]
            else:
                pass     
    else:
        pass
    return cplace


def logToFile(logfile, message):
    logf=open(logfile, "a+")
    logf.write("\n"+message)