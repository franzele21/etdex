"""
This program calls the API that retrieve all AFTN messages
it needs an API Token, but for having this token, it needs a auth (see
get_token.py)
"""

import requests
import json
import os
import time
from functions import print_context


ACCESS_TOKEN_FILE = "access_token.json"
OUTPUT_FILE = "data_traffic.json"
MAX_ID_FILE = "max_id.txt"
with open(MAX_ID_FILE) as file:
    # the last id of the API, not the output file
    MAX_ID = int(file.read())
FILENAME = os.path.basename(__file__)
CYCLE_TIME = 3600

print_c = lambda text : print_context(FILENAME, text)

print_c("initialization")

def read_token_access_file(access_file: str) -> dict:
    """
Reads the file containing the access token to the database

Parameters
----------
access_file : str
    The file where the token is located

Returns
-------
dict
    a dict with the access token and its expiration date
    """
    with open(access_file) as file:
        content = json.loads(file.read())
    access_token = content["access_token"]
    expiration_date = content["expiration_date"]

    return {"access_token": access_token, "expiration_date": expiration_date}


def get_access_token() -> dict:
    """
Get the token (and its expiration date). If the file (where the token 
contained is) doesn't exist, it will be created, and the token will be
requested to the API (see get_token.py)

Returns
-------
dict
    a dict with the access token and its expiration date
    """
    try:
        # first we try normally to take the token
        get_access = read_token_access_file(ACCESS_TOKEN_FILE)
    except FileNotFoundError:
        # if it doesn't work, we create the file and request the token
        print_c("the access token file doesn't exists\nCreation of the file...")

        os.system("python3 get_token.py")
        get_access = read_token_access_file(ACCESS_TOKEN_FILE)

        print_c("File created")

    return get_access


def previous_been_read(output_file: str) -> tuple:
    """
This program will see if the output file has been read or not.
For this, the output file must be format:
{
    "been_read": true/false,
    "new_aftn": ...
}

Parameters
----------
output_file : str
    Path of the file, where the output data is stored

Returns
-------
tuple
    Tuple with this form "(<been_read>, <max_id>)", with been_read as
    True if it has been read (else False) and max_id the last id in
    the file (0 with it has been read, else the last id of the file)
    """
    try:
        with open(output_file) as file:
            content = json.loads(file.read())
            if content["been_read"]:
                # if it has been read, we don't care about the last id
                # so we give back 0
                return (True, 0)
            else:
                # if it hasn't been read, we get the last id of the 
                # file
                if len(content["new_aftn"].keys()) > 0:
                    max_id = max([int(id) for id in content["new_aftn"].keys()])
                else:
                    max_id = 0
                return (False, max_id)
    except FileNotFoundError:
        return (True, 0)


def traffic_search(token: str) -> int:
    """
Makes the requests at the AFTN API

Parameters
----------
token : str
    Access token needed to send requests to the API

Returns
int
    Status code fo the request
    """
    link = "https://aftn.pno.cloud/aftnmailbox/trafficfpl/read_one.php"
    header = {"Authorization": f"Bearer {token}"}
    queries = {"id": MAX_ID}

    # retrieve the status of the file (been read or not) and if it has
    # been read, the max id (else 0)
    been_read, max_id = previous_been_read(OUTPUT_FILE)

    # when we will hit the last id of the API, it will set is_not_max
    # to False
    is_not_max = True
    new_aftn = {}
    # we set the index to the max_id of the output file (if it hasn't 
    # been read, it will be the id of the last AFTN message + 1,
    # else 1)
    index = max_id + 1
    while is_not_max:
        response = requests.get(link, headers=header, params=queries)
        # we retrieve the AFTN message and the status code
        data, status = json.loads(response.text), response.status_code


        if status == 200:
            # if the request was successful
            # we get to the next id
            queries["id"] += 1
            # we save temporarily the AFTN message
            new_aftn[str(index)] = data["document"]
            # we add 1 to the index of the output file
            index += 1
        else:
            # an error means that there isn't an AFTN message at this 
            # id, so we break the while loop 
            is_not_max = False

    print_c(f"last id = {queries['id']}")

    # we save the last id for the API
    with open(MAX_ID_FILE, "w") as file:
        file.write(str(queries["id"]))

    if not been_read:
        # if the output file hasn't been read, we will merge the 
        # previous AFTN messages with the new one
        with open(OUTPUT_FILE) as file:
            previous_aftn = json.loads(file.read())["new_aftn"]
        new_aftn = {**previous_aftn, ** new_aftn}

    # we create the output data, and put the been_read info to False
    output_data = {
        "been_read": False,
        "new_aftn": new_aftn
    }
    with open(OUTPUT_FILE, "w+") as file:
        file.write(json.dumps(output_data))

    return status


access = get_access_token()
while True:
    print_c("begin of the routine")

    if time.time() > int(access["expiration_date"]):
        # this code will be executed when we reach the expiration date
        # it will ask the API for a new API key
        print("getting a new key...")
        os.system("python3 get_token.py")
        access = get_access_token()
        print(f"the new key exprires at: {access['expiration_date']}")
    

    traffic_search(access['access_token'])

    print_c("end of the routine")
    time.sleep(CYCLE_TIME)
