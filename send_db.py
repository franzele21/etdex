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

all_airplanes = query("SELECT * FROM \"TREATED_DATA\" WHERE tdSent = '0';")

if all_airplanes and all_airplanes != "locked": 
    all_airplanes = all_airplanes.fetchall()
else:
    print_context(FILENAME, "Error: no new airplane found", True)
    conn.close()
    exit()

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

counter = 0
for airport, password in airport_to_send.items():
    print_c(f"Sending movement from {airport}...")
    new_movement = query(f"SELECT * FROM \"TREATED_DATA\" WHERE tdSent = '0' AND tdAirport = '{airport}';")
    if new_movement:
        for movement in new_movement:
            tmp_json = format_json.copy()

            airplane_time = datetime.utcfromtimestamp(movement[3]).strftime("%Y-%m-%dT%H:%m:%S+00:00")
            tmp_json["createdAt"] = airplane_time
            tmp_json["airport"] = movement[1]
            tmp_json["aircraftName"] = movement[2]
            tmp_json["callsign"] = movement[2]
            tmp_json["additionalInformation"] = f"{{prob: {movement[4]}}}"

            response = requests.post(URL, data=tmp_json, auth=(airport, password))

            if response.status_code == 200:
                print_c(f"Movement sent: {movement[2]} to {movement[1]} at {airplane_time}")
                query(f"""
                        UPDATE "TREATED_DATA"
                        SET tdSent = '1'
                        WHERE tdId = '{movement[0]}';
                    """)
                counter += 1
            else:
                print_c(f"Error: {response} for {movement[0]}\n{response.text}")

print_c(f"Number of sent movements: {counter}")
conn.close()
