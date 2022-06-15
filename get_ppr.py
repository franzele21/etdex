"""
With this program you can retrieve PPRs
Note that you have to have a account toavdb (to retrieve the airports),
and one to the PPR API.
Note that this program can just function with a begin date and an end
date
"""
import requests
import json
import time

# path to the id for the ppr api
PPR_AUTH_PATH = "auth_ppr.json"
# path to the id for the airport api
AIRPORT_AUTH_PATH = "auth_avdb.json"
# the PPR needs to be younger than this variable in hour
MAXIMUM_PPR_OLD = 2
# the status of the ppr
PPR_STATUS = "new"
# output file, where the PPR is going to be kept
OUTPUT_FILE = "output_ppr.json"


# we will first retrieve the id and password for the PPR API and for 
# the airport API
with open(PPR_AUTH_PATH) as file:
    content = json.loads(file.read())
    user_ppr, password_ppr = content["user"], content["password"]

with open(AIRPORT_AUTH_PATH) as file:
    content = json.loads(file.read())
    user_avdb, password_avdb = content["user"], content["password"]

# we retrieve the airports
response = requests.get("https://avdb.aerops.com/public/airports", 
                        auth=(user_avdb, password_avdb))
airports = json.loads(response.text)["data"]

# we keep just the names
airports_name = [airport["name"] for airport in airports]

while True:

    # we retrieve the PPRs
    # in the query, we give which status we want, and from when to when
    queries = {
        "status": PPR_STATUS,
        "beforeTimestamp": int(time.time()),
        "afterTimestamp": int(time.time()) - (MAXIMUM_PPR_OLD * 60 * 60)}
    response = requests.get("https://dev1.avdb.aerops.com/public/ppr-data",
                            params=queries,
                            auth=(user_ppr, password_ppr))

    ## if the auth weren't correct, it will display an error 401
    if response.text != "":
        multiple_ppr = json.loads(response.text)
    else:
        print(response, "couldn't load any PPRs")
        multiple_ppr = ""

    # we verify that the PPR are valid (that the airport where it landed or
    # destination airport exists)
    new_ppr = {str(index): ppr for index, ppr in enumerate(multiple_ppr)
                            if ppr["departingTo"].strip().upper() in airports_name
                            or ppr["airport"].strip().upper() in airports_name
                            and len(ppr["eventTimestamp"]) > 1}                  

    # we normalize the airports (to upper, and without any 
    # spaces)
    for index in new_ppr:
        new_ppr[index]["departingTo"] = new_ppr[index]["departingTo"].strip().upper()
        new_ppr[index]["airport"] = new_ppr[index]["airport"].strip().upper()

    # we delete all ppr with a departing airport, but with not with a valid
    # departure time or present airport
    to_delete = []
    for index in new_ppr:
        if new_ppr[index]["departingTo"] in airports_name \
                and new_ppr[index]["airport"] not in airports_name \
                and new_ppr[index]["departure"] == "":     
            to_delete.append(index)
    for index in to_delete:
        del new_ppr[index]

    # we delete now all ppr with a valid present airport, but with no 
    # arrival time
    to_delete = []
    for index in new_ppr:
        if new_ppr[index]["airport"] in airports_name \
                and new_ppr[index]["arrival"] == "":
            to_delete.append(index)
    for index in to_delete:
        del new_ppr[index]


    # we write it in the output file  
    with open(OUTPUT_FILE, "w+") as file:
        file.write(json.dumps(new_ppr, indent=2))

    time.sleep(MAXIMUM_PPR_OLD * 60 * 60 )
