import os
import json
import requests
from functions import *
from datetime import datetime

FILENAME = os.path.basename(__file__)
DATABASE_PATH = "database.db"
URL = "https://dev1.avdb.aerops.com/public/v2/airport/movement"
AIRPORT_SEND_FILE = "airport_to_send.json"

print_c = lambda text : print_context(FILENAME, text)

conn = create_connection(DATABASE_PATH)
query = lambda x: query_to_bdd(conn, FILENAME, x)

# we retrieve all treated movement, that weren't sent
all_airplanes = query("SELECT * FROM \"TREATED_DATA\" WHERE tdSent = '0';")

# we close the program if there was a problem, else, we fetchall data
if all_airplanes: 
    all_airplanes = all_airplanes.fetchall()
else:
    conn.close()
    exit()

# we look up which for which airport we need to send the data
with open(AIRPORT_SEND_FILE) as file:
    airport_to_send = json.loads(file.read())

format_json = {
   "createdAt":"",                          # to add
   "createdBy":"etdex",
   "airport":"",                            # to add
   "airtrackCustomerId":None,
   "flighttype":"",
   "navigation":"",
   "trafficstatus":"",
   "joiningpoints":"",
   "canceled":None,
   "pax":None,
   "crew":None,
   "parkingMinutes":None,
   "parkingHours":None,
   "parkingMonths":None,
   "parkingDays":None,
   "children":None,
   "flighttypeCat":"",
   "aircraftName":"",                       # to add
   "runway":"",
   "flightFrom":"",
   "flightTo":"",
   "flightId":"",
   "aircraftType":"",
   "flightTimeEstimated":None,
   "dof":"",
   "flightlevel":"",                        # add ?
   "speed":"",                              # add ?
   "flightway":"",
   "flightheight":"",
   "weightCat":"",
   "properties":"",
   "vfrPoint":"",
   "locked":"",
   "flightplan":None,
   "flightplanText":"",
   "tagged":"",
   "computerName":"",
   "departure":"",
   "lastinfo":"",
   "pilot":"",
   "squawk":"",
   "infoText":"",
   "atis":"",
   "smap":"",
   "qnh":"",
   "slot":"",
   "readback":"",
   "executor":"",
   "freight":"",
   "generalRemarks":"",
   "uniqueId":"",
   "overdue":"",
   "startDatetime":"",
   "flighttime":"",
   "callsign":"",                                   # to add
   "eta":"",
   "ets":"",
   "landingDatetime":"",
   "flightDuration":None,
   "status":"",
   "customerId":None,
   "additionalInformation":""                       # to add
}

# next we parse every airport name, and see if there is data for that
counter = 0
for airport, password in airport_to_send.items():
    print_c(f"Sending movement from {airport}...")
    new_movement = query(f"SELECT * FROM \"TREATED_DATA\" WHERE tdSent = '0' AND tdAirport = '{airport}';")
    if new_movement:
        for movement in new_movement:
            tmp_json = format_json.copy()

            # we add all data from the landing to an empty dict
            airplane_time = datetime.utcfromtimestamp(movement[3]).strftime("%Y-%m-%dT%H:%m:%S+00:00")
            tmp_json["createdAt"] = airplane_time
            tmp_json["airport"] = movement[1]
            tmp_json["aircraftName"] = movement[2]
            tmp_json["callsign"] = movement[2]
            tmp_json["additionalInformation"] = f"{{prob: {movement[4]}}}"

            response = requests.post(URL, data=tmp_json, auth=(airport, password))

            if response.status_code == 200:
                # if the landing has been sent, we print it ...
                print_c(f"Movement sent: {movement[2]} to {movement[1]} at {airplane_time}")
                # ... update the movement to "sent" ...
                query(f"""
                        UPDATE "TREATED_DATA"
                        SET tdSent = '1'
                        WHERE tdId = '{movement[0]}';
                    """)
                # and add one to the counter
                counter += 1
            else:
                # if the status code isn't 200, print a error message
                # with the database id of the movement
                print_c(f"Error: {response} for {movement[0]}\n{response.text}")

print_c(f"Number of sent movements: {counter}")
conn.close()
