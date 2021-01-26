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
