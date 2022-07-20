"""
This program calls all programs of the fly-tracker block after a 
certain time.
Why this program? Because when multiple programs connect to a single 
database, it can create conflicts. So this program calls the program
one by one, so the conflict won't happen.

If you want to add a program that needs to be called (a new 
get_airplanes for instance) you have to write it the CALL_PATH file.
"""

import time 
import os
import json

# this file is where the programs that need to be called are written
# in this format:
# {
#   "x": {
#       "name": <Program_name>,
#       "cycle_time": <Cycle_seconds>
#   },
CALL_PATH = "call.json"

with open(CALL_PATH) as file:
    files = json.loads(file.read())


initialization = True
while True:
    if initialization:
        # just for the initialization, we execute all file, without 
        # checking the cycle time
        initialization = False
        for file in files.values():
            os.system(f"python3 {file['name']}")
            file["last_time"] = int(time.time())

    for file in files.values():
        if int(time.time()) > file["cycle_time"] + file["last_time"]:
            # if the time right now is higher than the last time the 
            # program was called + its cycle time, it will be executed
            os.system(f"python3 {file['name']}")
            file["last_time"] = int(time.time())

    time.sleep(1)
