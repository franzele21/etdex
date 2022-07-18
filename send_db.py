"""
This program sends all landing that are in the database (table 
TREATED_DATA) that weren't send (tdSent = 0) and that with the login 
in airport_to_send.json (tdAirport in airport_to_send)
"""

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

# we look up which for which airport we need to send the data
with open(AIRPORT_SEND_FILE) as file:
    airport_to_send = json.loads(file.read())


# next we parse every airport name, and see if there is data for that
counter = 0
for airport, password in airport_to_send.items():
    new_movement = query(f"SELECT * FROM \"TREATED_DATA\" WHERE tdSent = '0' AND tdAirport = '{airport}';")

    if new_movement:

        first_movement = True
        for movement in new_movement:
            if first_movement:
                print_c(f"Sending movement from {airport}...")
                first_movement = False

            format_json = {
                    "createdAt":None,                          # to add
                    "createdBy":"etdex",
                    "airport":None,                            # to add
                    "airtrackCustomerId":None,
                    "flighttype":None,
                    "navigation":None,
                    "trafficstatus":None,
                    "joiningpoints":None,
                    "canceled":None,
                    "pax":None,
                    "crew":None,
                    "parkingMinutes":None,
                    "parkingHours":None,
                    "parkingMonths":None,
                    "parkingDays":None,
                    "children":None,
                    "flighttypeCat":None,
                    "aircraftName":None,                       # to add
                    "runway":None,
                    "flightFrom":None,
                    "flightTo":None,
                    "flightId":None,
                    "aircraftType":None,
                    "flightTimeEstimated":None,
                    "dof":None,
                    "flightlevel":None,                        # add ?
                    "speed":None,                              # add ?
                    "flightway":None,
                    "flightheight":None,
                    "weightCat":None,
                    "properties":None,
                    "vfrPoint":None,
                    "locked":None,
                    "flightplan":None,
                    "flightplanText":None,
                    "tagged":None,
                    "computerName":None,
                    "departure":None,
                    "lastinfo":None,
                    "pilot":None,
                    "squawk":None,
                    "infoText":None,
                    "atis":None,
                    "smap":None,
                    "qnh":None,
                    "slot":None,
                    "readback":None,
                    "executor":None,
                    "freight":None,
                    "generalRemarks":None,
                    "uniqueId":None,
                    "overdue":None,
                    "startDatetime":None,
                    "flighttime":None,
                    "callsign":None,                                   # to add
                    "eta":None,
                    "ets":None,
                    "landingDatetime":None,
                    "flightDuration":None,
                    "status":None,
                    "customerId":None,
                    "additionalInformation":None                       # to add
                }

            # we add all data from the landing to an empty dict
            airplane_time = datetime.utcfromtimestamp(movement[3]).strftime("%Y-%m-%dT%H:%m:%S+00:00")
            format_json["createdAt"] = airplane_time
            format_json["airport"] = movement[1]
            format_json["aircraftName"] = movement[2]
            format_json["callsign"] = movement[2]
            format_json["additionalInformation"] = f"{{prob: {movement[4]}}}"

            format_json = json.dumps(format_json)

            response = requests.post(URL, data=format_json, auth=(airport, password))

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
