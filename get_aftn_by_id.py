import requests
import json
import os
import time
from functions import print_context


ACCESS_TOKEN_FILE = "access_token.json"
OUTPUT_FILE = "data_traffic.json"
MAX_ID_FILE = "max_id.txt"
with open(MAX_ID_FILE) as file:
    MAX_ID = int(file.read())
FILENAME = os.path.basename(__file__)
CYCLE_TIME = 3600

print_context(FILENAME, "initialization")

def read_token_access_file(access_file):
    with open(access_file) as file:
        content = json.loads(file.read())
    access_token = content["access_token"]
    expiration_date = content["expiration_date"]

    return {"access_token": access_token, "expiration_date": expiration_date}


def get_access_token():
    try:
        get_access = read_token_access_file(ACCESS_TOKEN_FILE)
    except FileNotFoundError:
        print("the access token file doesn't exists\nCreation of the file...")

        os.system("python3 get_token.py")
        get_access = read_token_access_file(ACCESS_TOKEN_FILE)

        print("File created")

    return get_access


def previous_been_read(output_file):
    try:
        with open(output_file) as file:
            content = json.loads(file.read())
            if content["been_read"]:
                return (True, 0)
            else:
                if len(content["new_aftn"].keys()) > 0:
                    max_id = max([int(id) for id in content["new_aftn"].keys()])
                else:
                    max_id = 0
                return (False, max_id)
    except FileNotFoundError:
        return (True, 0)


def traffic_search(access):
    link = "https://aftn.pno.cloud/aftnmailbox/trafficfpl/read_one.php"
    header = {"Authorization": f"Bearer {access['access_token']}"}
    queries = {"id": MAX_ID}

    been_read, max_id = previous_been_read(OUTPUT_FILE)

    is_not_max = True
    new_aftn = {}
    index = max_id
    while is_not_max:
        response = requests.get(link, headers=header, params=queries)
        data, status = json.loads(response.text), response.status_code


        if status == 200:
            queries["id"] += 1
            new_aftn[str(index)] = data["document"]
            index += 1
        else:
            is_not_max = False

    print_context(FILENAME, F"last id = {queries['id']}")
    with open(MAX_ID_FILE, "w") as file:
        file.write(str(queries["id"]))

    if not been_read:
        with open(OUTPUT_FILE) as file:
            previous_aftn = json.loads(file.read())["new_aftn"]
        new_aftn = {**previous_aftn, ** new_aftn}

    output_data = {
        "been_read": False,
        "new_aftn": new_aftn
    }
    with open(OUTPUT_FILE, "w+") as file:
        file.write(json.dumps(output_data))

    return status


access = get_access_token()
while True:
    print_context(FILENAME, "begin of the routine")

    if time.time() > int(access["expiration_date"]):
        print("getting a new key...")
        os.system("python3 get_token.py")
        access = get_access_token()
        print(f"the new key exprires at: {access['expiration_date']}")
    

    traffic_search(access)

    print_context(FILENAME, "end of the routine")
    time.sleep(CYCLE_TIME)
