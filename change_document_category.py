#!/usr/bin/python3.6
# Document Category Change script
# it converts letters to avvisi and avvisi to letters
# Author: Lorenzo Allori <lorenzo.allori@gmail.com>
import os
import sys
from io import open, StringIO
from datetime import datetime
import json
from configparser import ConfigParser
import mysql.connector
from mysql.connector import Error

# Script parameters
script_name=os.path.basename(__file__)

if len(sys.argv) != 2: 
    print("Usage: python3 "+script_name+" (toavviso|toletter)")
    exit() 

arg = str(sys.argv[1])

# Files section
# configFile="configFile_TESTING.ini"
configFile="configFile.ini"
avvisifile="avvisilist.txt"

# ConfigParser get config properties
host=str()
port=str()
user=str()
passwd=str()
database=str()

if os.path.isfile(configFile):
    parser = ConfigParser()
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
        sys.exit()

# Get Date and Time
def currentDateAndTime():
    now = datetime.now()
    current_date = now.strftime("%Y-%m-%d")
    current_time = now.strftime("%H:%M:%S")
    dateAndTime=current_date+" "+current_time
    return dateAndTime


# Get list
def get_avvisi_list():
    avvisi_file=open(avvisifile, "r")
    avvisilist = []
    for line in avvisi_file:
        stripped_line = line.strip()
        line_list = stripped_line.split()
        avvisilist.append(line_list)
    avvisi_file.close()
    return avvisilist

# Clean entry (remove : and , and s and u)
def clean_entry(entryvalue):
    entryvalue=entryvalue.strip()
    entryvalue=entryvalue.replace(",","")
    entrylist = entryvalue.split(":")
    entryid=entrylist[0]
    unsure=entrylist[1]
    return entryid, unsure

def check_recipient(entry):
    #(3905 = not relevant in this entry  - 9285 = person name lost)
    excludedtermlist="3905,9285,NULL"
    listexcl=excludedtermlist.split (",")
    if entry in listexcl:
        exclude_recipient="1"
    else:
        exclude_recipient="0"
    return exclude_recipient

def check_recipient_place(entry):
    #(54332 = not relevant in this entry - 53384 = 'place name lost') 
    excludedtermlist="54332,53384,NULL"
    listexcl=excludedtermlist.split (",")
    if entry in listexcl:
        exclude_recipientPlace="1"
    else:
        exclude_recipientPlace="0"
    return exclude_recipientPlace

def get_completeNewsFromValue(miaDocId,mycursor):
    q_completeNewsFromValue="select newsFrom from tblDocNews where documentEntityId="+miaDocId
    mycursor.execute(q_completeNewsFromValue)
    completeNewsFromValue = mycursor.fetchall()
    if completeNewsFromValue:
        completeNewsFromValue = completeNewsFromValue[0][0]
    # else:
    #     completeNewsFromValue=[]
    return completeNewsFromValue

def execute_query(mycursor,query,message):
    try:
        mycursor.execute(query)
        # mydb.commit()
        # print('Commit to DB: [OK]')        
    except Error as e:
        # mydb.rollback()
        # print('Commit to DB: [KO] - Rollback')
        print('###ERROR!!')
        print("QUERY: "+query)
        print("ERROR:"+message+" -> "+str(e))
        sys.exit()

def convert_letters_to_avvisi(miaDocId):
    # Define Queryset
    q_cat_typ = "SELECT category, typology FROM tblDocumentEnts where documentEntityId="+miaDocId
    q_recipient= "SELECT recipient FROM tblDocCorrespondence where documentEntityId="+miaDocId
    q_plOfOrig_plOfOrigUns="SELECT placeOfOrigin,placeOfOriginUnsure FROM tblDocumentEnts where documentEntityId="+miaDocId
    q_recipientPlace = "SELECT recipientPlace FROM tblDocCorrespondence where documentEntityId="+miaDocId

    # Serialized Action description
    serialized_data={
            "basicDescription":{
                "edit":{
                    "before":{
                        "typology":"Correspondence-Letter"
                    },
                    "after":{
                        "typology":"News-Avviso"
                    }
                }
            }
        }
    serialized_data=json.dumps(serialized_data)

    try:
        mydb = mysql.connector.connect(
        host=myhost,
        port=myport,
        user=myuser,
        passwd=mypasswd,
        database=mydbname
        )
        mycursor = mydb.cursor()

        mycursor.execute(q_cat_typ)
        cat_typ = mycursor.fetchall()
        category=cat_typ[0][0]
        typology=cat_typ[0][1]

        if typology is None or category is None:
            print('Document ' + miaDocId + ' category or typology is None --- pls check document --- exiting ')

        if typology != "Letter":
            print('Document ' + miaDocId + ' is NOT a Letter - Record not touched')
        else:
            # Get recipient person
            mycursor.execute(q_recipient)
            recipientl = mycursor.fetchall()
            if recipientl:
                if recipientl[0][0] is None:
                    pass
                else:
                    recipient,recipientUnsure = clean_entry(recipientl[0][0])
    
            # Get recipient place
            mycursor.execute(q_recipientPlace)
            recipientPlacel = mycursor.fetchall()
            if recipientPlacel:
                if recipientPlacel[0][0] is None:
                    pass
                else:
                    recipientPlace, recipientPlaceUnsure = clean_entry(recipientPlacel[0][0])

            # Get place of Origin
            mycursor.execute(q_plOfOrig_plOfOrigUns)
            plOfOriginl = mycursor.fetchall()
            if plOfOriginl:
                plOfOrigin=plOfOriginl[0][0]
                plOfOriginUnsure=plOfOriginl[0][1]

                if plOfOriginUnsure == "u":
                    pass
                else:
                    plOfOriginUnsure = 's'
            
            ## Commit INSERTS to db
            ## Initialize tblDocNews
            # Creating empty line in tblDocNews
            q_insert_recipient_place_tblDocNews="INSERT INTO mia.tblDocNews (documentEntityId,newsFrom) VALUES ("+miaDocId+", NULL);"
            # RUN QUERY
            execute_query(mycursor,q_insert_recipient_place_tblDocNews,"Initialize tblDocNews")

            # Check Recipient Person
            try: recipient
            except NameError: print('') #TODO we could log this into a file
            else:
                exclude_recipient=check_recipient(recipient)
                if exclude_recipient == "0":
                    q_insert_recipient_tblRefToPeople="INSERT INTO mia.tblDocumentEntsRefToPeople (documentId, peopleId, unSure) VALUES ("+miaDocId+", "+recipient+", "+"'"+recipientUnsure+"'"+");"
                    # RUN QUERY
                    execute_query(mycursor,q_insert_recipient_tblRefToPeople,"Check Recipient Person")
            
            # Check Recipient Place
            try: recipientPlace
            except NameError: print('') #TODO we could log this into a file
            else:
                exclude_recipientPlace=check_recipient_place(recipientPlace)
                if exclude_recipientPlace == "0":
                    q_insert_recipient_place_tblDocNews="INSERT INTO mia.tblDocNews (documentEntityId,newsFrom) VALUES ("+miaDocId+", '"+recipientPlace+":"+recipientPlaceUnsure+",');"
                    execute_query(mycursor,q_insert_recipient_place_tblDocNews,"Check Recipient Place - Unsure")
                else:    
                    print("Creating empty line in tblDocNews")
                    q_insert_recipient_place_tblDocNews="INSERT INTO mia.tblDocNews (documentEntityId,newsFrom) VALUES ("+miaDocId+", NULL);"
                    # RUN QUERY
                    execute_query(mycursor,q_insert_recipient_place_tblDocNews,"Check Recipient Place")

            # Check Place of Origin
            try: plOfOrigin
            except NameError: print('')
            else:
                exclude_plOfOrigin=check_recipient_place(plOfOrigin)
                if exclude_plOfOrigin == "0":
                    completeNewsFromValue = get_completeNewsFromValue(miaDocId,mycursor)
                    if completeNewsFromValue is None:
                        if plOfOrigin is None:
                            pass
                        else:
                            addPlOfOriginToNewsFromValue=str(plOfOrigin)+':'+plOfOriginUnsure+','
                            q_update_newfrom_tblDocNews="UPDATE mia.tblDocNews SET newsFrom='"+addPlOfOriginToNewsFromValue+"' WHERE documentEntityId ="+miaDocId
                            # RUN QUERY
                            execute_query(mycursor,q_update_newfrom_tblDocNews,"Check Place of Origin - add in newFrom")
                    else:
                        addPlOfOriginToNewsFromValue=str(completeNewsFromValue)+str(plOfOrigin)+':'+plOfOriginUnsure+','
                        q_update_newfrom_tblDocNews="UPDATE mia.tblDocNews SET newsFrom='"+addPlOfOriginToNewsFromValue+"' WHERE documentEntityId ="+miaDocId
                        # RUN QUERY
                        execute_query(mycursor,q_update_newfrom_tblDocNews,"Check Place of Origin - add in newFrom")
                else:
                    # Remove "not relevant in this entry" or "place name lost" from placeOfOrigin field in tblDocumentEnts
                    q_update_places_tblDocumentEnts="UPDATE mia.tblDocumentEnts SET placeOfOrigin = NULL where documentEntityId="+miaDocId
                    # RUN QUERY
                    execute_query(mycursor,q_update_places_tblDocumentEnts,"Check Place of Origin - remove from newFrom")
            
            
            # Get and Add places in topic list assigned to Avvisi Topic
            q_get_places_from_avvisi_topic="SELECT placeId FROM tblDocumentTopicPlace WHERE (documentEntityId ='"+miaDocId+"' AND topicListId = '53')"
            mycursor.execute(q_get_places_from_avvisi_topic)
            tPlacesList = mycursor.fetchall()
            completeNewsFromValue = get_completeNewsFromValue(miaDocId,mycursor)
            if plOfOriginl:
                for placeId in tPlacesList:
                    topicPlace=placeId[0]
                    if completeNewsFromValue is None:
                        completeNewsFromValue=str(topicPlace)+':s,'
                    else:
                        completeNewsFromValue=completeNewsFromValue+str(topicPlace)+':s,'  

                    newNewsFromValue=completeNewsFromValue
                    # Removing duplicates in list
                    newNewsFromValue = newNewsFromValue[:-1]
                    distinct_newNewsFromValue = ','.join(set(newNewsFromValue.split(',')))+','
                    q_update_newfrom_tblDocNews="UPDATE mia.tblDocNews SET newsFrom='"+distinct_newNewsFromValue+"' WHERE documentEntityId ="+miaDocId
                    # RUN QUERY
                    execute_query(mycursor,q_update_newfrom_tblDocNews,"Check Place of Origin - add Avvisi Topic in newFrom")
            else:
                pass
            # Change Document Category to News and Document Typology to Avviso
            q_change_cat_typ="UPDATE mia.tblDocumentEnts SET category='News', typology='Avviso' WHERE  documentEntityId="+miaDocId
            # RUN QUERY
            execute_query(mycursor,q_change_cat_typ,"Change Document Category and Typology")

            # Clean stuff in tblDocCorrespondence
            q_clean_tblDocCorrespondence="DELETE FROM mia.tblDocCorrespondence WHERE documentEntityId="+miaDocId
            # RUN QUERY
            execute_query(mycursor,q_clean_tblDocCorrespondence,"Clean stuff in tblDocCorrespondence")

            # Check Sender (Compiler) (3905 = not relevant in this entry  - 9285 = person name lost)
            try: recipient
            except NameError: print('')
            else:
                exclude_compiler=check_recipient(recipient)
                if exclude_compiler == "0":
                    pass
                else:
                    q_remove_compiler="DELETE FROM mia.tblDocumentsEntsPeople WHERE documentEntityId="+miaDocId
                    # RUN QUERY
                    execute_query(mycursor,q_remove_compiler,"Check Sender (Compiler)")

            # Remove Topic Avvisi from transformed Letter to Avviso
            q_remove_avvisitopics="DELETE FROM mia.tblDocumentTopicPlace WHERE  documentEntityId="+miaDocId+" AND topicListId=53;"
            # RUN QUERY
            execute_query(mycursor,q_remove_avvisitopics,"Remove Topic Avvisi")

            # Add action to tblLastAccessedItems
            dateAndTime=currentDateAndTime()+".0"
            q_insert_action="INSERT INTO mia.tblLastAccessedRecords (account,recordType,action,dateAndTime,entryId,serializedData) VALUES ('pteam','DE','EDIT','"+dateAndTime+"','"+miaDocId+"','"+str(serialized_data)+"');"
            execute_query(mycursor,q_insert_action,"Add action to tblLastAccessedItems")

            # Commit Changes to db
            try:
                mydb.commit()
                print("Document " + miaDocId + " trasformed into News/Avviso -- commit to DB: [OK]")
            except:
                mydb.rollback()
                print("Commit to DB: [KO] - Rollback")

    except Error as e:
        print("Error while connecting to MySQL", e)


def convert_avvisi_to_letters(miaDocId):
    # Define Queryset
    q_cat_typ = "SELECT category, typology FROM tblDocumentEnts where documentEntityId="+miaDocId

    # Serialized Action description
    serialized_data={
            "basicDescription":{
                "edit":{
                    "before":{
                        "typology":"News-Avviso"
                    },
                    "after":{
                        "typology":"Correspondence-Letter"
                    }
                }
            }
        }
    serialized_data=json.dumps(serialized_data)

    try:
        mydb = mysql.connector.connect(
        host=myhost,
        port=myport,
        user=myuser,
        passwd=mypasswd,
        database=mydbname
        )
        mycursor = mydb.cursor()

        mycursor.execute(q_cat_typ)
        cat_typ = mycursor.fetchall()
        category=cat_typ[0][0]
        typology=cat_typ[0][1]

        if typology is None or category is None:
            print('Document ' + miaDocId + ' category or typology is None --- pls check document --- exiting ')

        if typology != "Avviso":
            print('Document ' + miaDocId + ' is NOT an Avviso - Record not touched')
        else:
            ## Get placeid values to populate tblDocumenTopicPlace with places and topic avvisi
            # Get newsFrom value in tblDocNews
            completeNewsFromValue = get_completeNewsFromValue(miaDocId,mycursor)
            if completeNewsFromValue is None:
                pass
            else:
                newsFromValue=completeNewsFromValue.replace(":s","")
                newsFromValue=newsFromValue.replace(":u","")
                newsFromValue=newsFromValue[:-1]

                # Create list from values in newsFrom
                topicPlaceList = newsFromValue.split(',')
                
                ## Commit INSERTS to db
                # Initialize tblDocNews
                # Creating empty line in tblDocCorrespondence
                print("Creating empty line in tblDocCorrespondence")
                q_insert_recipient_place_tblDocNews="INSERT INTO mia.tblDocCorrespondence (documentEntityId,recipient) VALUES ("+miaDocId+", NULL);"
                # RUN QUERY
                execute_query(mycursor,q_insert_recipient_place_tblDocNews,"Initialize tblDocCorrespondence")

                # Create Avvisi Topics with place from newsFrom table
                for placeId in topicPlaceList:
                    topicPlace=placeId
                    q_insert_avvisitopic_tblDocumentTopicPlace="INSERT INTO mia.tblDocumentTopicPlace (documentEntityId,placeId,topicListId) VALUES ("+miaDocId+","+topicPlace+",53)"
                    execute_query(mycursor,q_insert_avvisitopic_tblDocumentTopicPlace,"Create Avvisi Topics - add Avvisi Topic")
                
            # Change Document Category to News and Document Typology to Avviso
            q_change_cat_typ="UPDATE mia.tblDocumentEnts SET category='Correspondence', typology='Letter' WHERE  documentEntityId="+miaDocId
            # RUN QUERY
            execute_query(mycursor,q_change_cat_typ,"Change Document Category and Typology")

            # Clean stuff in tblDocNews
            q_clean_tblDocNews="DELETE FROM mia.tblDocNews WHERE documentEntityId="+miaDocId
            # RUN QUERY
            execute_query(mycursor,q_clean_tblDocNews,"Clean stuff in tblDocCorrespondence")

            # Add action to tblLastAccessedItems
            dateAndTime=currentDateAndTime()+".0"
            q_insert_action="INSERT INTO mia.tblLastAccessedRecords (account,recordType,action,dateAndTime,entryId,serializedData) VALUES ('pteam','DE','EDIT','"+dateAndTime+"','"+miaDocId+"','"+str(serialized_data)+"');"
            execute_query(mycursor,q_insert_action,"Add action to tblLastAccessedItems")

            # Commit Changes to db
            try:
                mydb.commit()
                print("Document " + miaDocId + " trasformed into Correspondence/Letter -- commit to DB: [OK]")
            except:
                mydb.rollback()
                print("Commit to DB: [KO] - Rollback")

    except Error as e:
        print("Error while connecting to MySQL", e)

#### RUN
def main():
    avvisilist=get_avvisi_list()
    for miaDocId in avvisilist:
        if arg == 'toavviso':
            convert_letters_to_avvisi(miaDocId[0])
        elif arg == 'toletter':
            convert_avvisi_to_letters(miaDocId[0])
        else:
            print(arg +" option not sopported")
            print("Usage: python3 "+script_name+" (toavviso|toletter)")
            exit()

if __name__ == "__main__":
    main()