#!/usr/bin/python3
# Utility fuctions - random stuff
# Author Lorenzo Allori <lorenzo.allori@gmail.com>

from datetime import datetime

# Get Date and Time
def currentDateAndTime():
    now = datetime.now()
    current_date = now.strftime("%Y-%m-%d")
    current_time = now.strftime("%H:%M:%S")
    return current_date, current_time


def filldate(s):
    d='/'.join(x.zfill(2) for x in d.split('/'))
    return s

def existstr(s):
    return '' if s is None else str(s)