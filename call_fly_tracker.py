import time 
import os
import json

CALL_PATH = "call.json"

with open(CALL_PATH) as file:
    files = json.loads(file.read())


initialization = True
while True:
    if initialization:
        initialization = False
        for file in files.values():
            os.system(f"python3 {file['name']}")
            file["last_time"] = int(time.time())

    for file in files.values():
        if int(time.time()) > file["cycle_time"] + file["last_time"]:
            os.system(f"python3 {file['name']}")
            file["last_time"] = int(time.time())
    time.sleep(1)
