"""
With this program, you can detect airplanes, using APIs, that detects 
them with the ADS-B signal.
All airplanes will be registered in a database, and when it will 
disappear from the ADS-B radar, it will be registered as "invisible 
airplane" in the database.
"""

import json
import time
import os
import requests
from functions import *

# path to the database
DATABASE_PATH = "airplane.db"
# api, from where the airplanes comes
SOURCE = "JetVision"
AUTH_FILE = "auth_api.json"
FILENAME = os.path.basename(__file__)
CYCLE_TIME = 30

print_c = lambda text : print_context(FILENAME, text)


print_c("initialization")

def to_dict_by_callsign(airplane_list: list, callsign: str, 
                        latitude: int, longitude: int, 
                        heading: int, geo_altitude: int,
                        velocity: int, airplane_time: int, 
                        ) -> dict:
    """
Returns a formated dictionary of airplanes, with they're callsign as key

Parameters
----------
airplane_list : list
    List of all airplanes
callsign : str or int
    Key or index to the callsign value
latitude : str or int
    Key or index to the latitude value
longitude : str or int
    Key or index to the longitude value
heading : str or int
    Key or index to the heading/direction value
geo_altitude : str or int
    Key or index to the altitude value
velocity : str or int
    Key or index to the velocity/speed value
airplane_time : str or int
    Key or index to the time value of the last contact

Returns
-------
dict
    Formated list of all airplane, with they're callsign as key
    """
    new_dict = {}
    for i in airplane_list:


        # in this part, we are going to see if all the data is available
        latitude_value = i[latitude] if not isinstance(i[latitude], str) else eval(i[latitude])
        longitude_value = i[longitude] if not isinstance(i[longitude], str) else eval(i[longitude])
        if isinstance(latitude_value, type(None)) or \
                isinstance(longitude_value, type(None)):
            continue

        heading_value = i[heading] if not isinstance(i[heading], str) else eval(i[heading])
        if isinstance(heading_value, type(None)) :
            continue

        geo_altitude_value = i[geo_altitude] if not isinstance(i[geo_altitude], str) else eval(i[geo_altitude])
        if isinstance(geo_altitude_value, type(None)):
            continue
        geo_altitude_value *= 0.3048    # to convert from feet to meters

        velocity_value = i[velocity] if not isinstance(i[velocity], str) else eval(i[velocity])
        if isinstance(velocity_value, type(None)):
            continue
        velocity_value *= 0.5144        # to convert from knots to meter per seconds


        # time is the only data that we can put by ourselves
        airplane_time_value = i[airplane_time]
        if isinstance(airplane_time_value, type(None)):
            airplane_time_value = int(time.time())

        if isinstance(i[callsign], type(None)):
            continue

        i[callsign] = i[callsign].replace("-", "")
        new_dict[i[callsign].strip()] = {"latitude": latitude_value, 
                                            "longitude": longitude_value,
                                            "heading": heading_value, 
                                            "altitude": geo_altitude_value, 
                                            "velocity": velocity_value, 
                                            "time": airplane_time_value}

    return new_dict

def initialize_database(conn: sqlite3.Connection) -> None:
    """
Creates the table AIRPLANE, if it doesn't exist
    """
    table_exists = query("SELECT count(name) FROM sqlite_master WHERE type = 'table' AND name = 'AIRPLANE';").fetchall()
    if table_exists[0][0] == 0:
        # we have the basic parameters of a airplane (registration 
        # name, coordinate, altitude, speed and heading) qnd three more
        # aspects : when it was last seen, if it is invisible, and since
        # when it's invisible
        query("""
                CREATE TABLE "AIRPLANE" ( 
                    "apRegis" TEXT NOT NULL, 
                    "apLatitude" REAL, 
                    "apLongitude" REAL, 
                    "apAltitude" REAL, 
                    "apTime" INTEGER,
                    "apVelocity" REAL,
                    "apHeading" REAL,
                    "apInvisible" INTEGER,
                    "apInvisibleTime" INTEGER,
                    "apSource" TEXT,
                    CONSTRAINT unique_direction UNIQUE (apRegis, apSource),
                    PRIMARY KEY ("apRegis", "apSource") );
            """)

with open(AUTH_FILE) as file:
    content = json.loads(file.read())[SOURCE]
    user = content["user"]
    api_key = content["key"]

conn = create_connection(DATABASE_PATH)
query = lambda query_ : query_to_bdd(conn, FILENAME, query_)
initialize_database(conn)
conn.close()

while True:
    print_c("begin of the routine")

    headers = {
        'Accept': 'application/json; charset=UTF-8',
    }
    
    auth = (user, api_key)

    response = requests.get('https://mlat.jetvision.de/mlat/aircraftlist.json', headers=headers, auth=auth)

    if response.status_code != 200:
        print_context("ERROR: there was a problem during the request (statuscode: {response.status_code})")
        print_c("anormal end of the routine")
        time.sleep(CYCLE_TIME)
        continue

    content = json.loads(response.text)
    
    # create a dict from the api data, indexed by they're licence number 
    airplane_data = to_dict_by_callsign(content, "reg", "lat", "lon", "trk", "alt", "spd", "uti")

    # creates the database if it doesn't exist
    conn = create_connection(DATABASE_PATH)
    query = lambda query_ : query_to_bdd(conn, FILENAME, query_)


    wait_unlock_db(query, DATABASE_PATH, FILENAME, SOURCE)

    initialize_database(conn)

    print_c(f"Number of new airplanes: {len(airplane_data.keys())}")
    for airplane_name in airplane_data.keys():
        unique_airplane = query(f"SELECT * FROM \"AIRPLANE\" WHERE apRegis = '{airplane_name}' AND apSource = '{SOURCE}';")
        unique_airplane = True if len(unique_airplane.fetchall()) == 0 else False
        tmp_airplane = airplane_data[airplane_name]

        # if the airplane isn't in the database, we add it
        if unique_airplane:
            query(f"""
                    INSERT INTO "AIRPLANE" VALUES
                    ('{airplane_name}', '{tmp_airplane["latitude"]}', 
                    '{tmp_airplane["longitude"]}', '{tmp_airplane["altitude"]}', 
                    '{int(tmp_airplane["time"])}', '{tmp_airplane["velocity"]}',
                    '{tmp_airplane["heading"]}', '0', '0', '{SOURCE}');
                    
                """)
        else:
            # if it does exists, we update it 
            # here we can see can we put apInvisible and apInivisibleTime 
            # to 0, because if the airplane wasn't on radar for a small
            # time, it will be registered as disappeared, but if it is 
            # in the list, it is reachable by ADS-B, so it isn't
            # invisible
            query(f"""
                    UPDATE "AIRPLANE"
                    SET apLatitude = '{tmp_airplane["latitude"]}',
                    apLongitude = '{tmp_airplane["longitude"]}',
                    apAltitude = '{tmp_airplane["altitude"]}',
                    apTime = '{tmp_airplane["time"]}',
                    apHeading = '{tmp_airplane["heading"]}',
                    apVelocity = '{tmp_airplane["velocity"]}',
                    apInvisible = '0',
                    apInvisibleTime = '0'
                    WHERE apRegis = '{airplane_name}'
                    AND apSource = '{SOURCE}';
                """)

    # if the airplane isn't to be seen by the ADS-B system,
    # it will be registered as invisible
    airplanes = query("SELECT apRegis, apSource FROM \"AIRPLANE\" WHERE apInvisible = '0';")
    for airplane in airplanes:
        if airplane[1] == SOURCE: 
            airplane_name = airplane[0]
            if airplane_name not in airplane_data.keys():
                query(f"""
                        UPDATE "AIRPLANE"
                        SET apInvisible = '1',
                        apInvisibleTime = '{int(time.time())}'
                        WHERE apRegis = '{airplane_name}'
                        AND apSource = '{SOURCE}';
                    """)
    conn.close()

    print_c("end of the routine")
    # it will pause 30 seconds, so we won't have any problem with the APIs
    time.sleep(CYCLE_TIME)
