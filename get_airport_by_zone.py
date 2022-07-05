"""
This program read the database, keep the airplane that aren't visible
by the ADS-B system.
After that, it creates a zone where the airplane can land, and see if
an airport is in this zone.
Once it found the airports for all invisible airplanes, it write in a 
JSON file the airplanes, and on which airports it could have landed.
"""
import requests
import json
import time
import os
from functions import *
from json import JSONDecodeError
from airplane_zone import create_zone
from in_polygon import is_inside_polygon


# database, where the airplanes are registered
AIRPLANE_DATABASE = "airplane.db"
# the path of the output file
OUTPUT_FILE = "airport_by_zone.json"
AUTH_AVDB_FILE = "auth_avdb.json"
FILENAME = os.path.basename(__file__)
CYCLE_TIME = 300

print_context(FILENAME, "initialization")

def filter_airports(airports: list) -> list:
    """
Filter out the airports with no exact coordinates

Parameters
----------
airports : list
    List of airports 

Returns
-------
list
    Filtered list of airports
    """
    new_list = [x for x in airports if not isinstance(x["latitude"], type(None))]
    new_list = [x for x in new_list if x["latitude"] != ""]

    new_list = [x for x in new_list if not isinstance(x["longitude"], type(None))]
    new_list = [x for x in new_list if x["longitude"] != ""]

    return new_list


def get_airplanes(file: str) -> list:
    """
Used to get the airplanes to analyse, and delete it after

Parameters
----------
file : str
    Filepath of the file with the data of the airplanes

Returns
-------
list
    Airplanes of the file
"""
    conn = create_connection(file)
    airplanes = query(conn, "SELECT * FROM \"INVISIBLE_AIRPLANE\" WHERE 1;")

    db_status = query(conn, """
                                INSERT INTO "AIRPLANE" 
                                VALUES ("{FILENAME}_", "", "", "", "", "", "", "", "", "");
                            """)
    query(conn, f"DELETE FROM \"AIRPLANE\" WHERE apRegis = \"{FILENAME}_\";")
    while not db_status:
        print_context(FILENAME, f"waiting for the {file} database to be unlocked")
        
        time.sleep(5)
        db_status = query(conn, f"""
                                    INSERT INTO "AIRPLANE" 
                                    VALUES ("{FILENAME}_", "", "", "", "", "", "", "", "", "");
                                """)
        query(conn, f"DELETE FROM \"AIRPLANE\" WHERE apRegis = \"{FILENAME}_\";")

    airplanes = airplanes.fetchall() if not isinstance(airplanes, type(None)) and airplanes else []

    query(conn, "DELETE FROM \"INVISIBLE_AIRPLANE\" WHERE 1;")
    
    conn.close()
    return airplanes


def format_airplanes(airplane_list: list) -> list:
    """
Formats the list, so the content is in the form of a dict

Parameters
----------
airplane_list : list
    List of airplane, with airplane represented as tuple

Returns
-------
list
    List of dictionnaries of airplanes
    """
    keys = ("callname", "latitude", "longitude", "altitude", "last_contact", "velocity", "heading", "invisible", "invisible_time")
    new_list = []
    for airplane in airplane_list:
        tmp_airplane = {}
        for i in range(len(airplane)):
            tmp_airplane[keys[i]] = airplane[i]
        new_list.append(tmp_airplane)

    return new_list


def previous_been_read(output_file):
    try:
        with open(output_file) as file:
            content = json.loads(file.read())
            if content["been_read"]:
                return True
            else:
                return False
    except FileNotFoundError:
        return True
    except JSONDecodeError:
        print_context(FILENAME, f"JSONDecodeError: will wait that {OUTPUT_FILE} will be ready")
        time.sleep(60)
        return previous_been_read(output_file)


with open(AUTH_AVDB_FILE) as file:
    content = json.loads(file.read())
    avdb_user = content["user"]
    avdb_password = content["password"]

# get all airports, and keep just the ones that have their coordinates
response = requests.get("https://avdb.aerops.com/public/airports", auth=(avdb_user, avdb_password))
airports = json.loads(response.text)["data"]
airports = filter_airports(airports)

while True:
    print_context(FILENAME, "begin of the routine")

    # get the airplanes that are inivisble by the ADS-B system
    airplanes = format_airplanes(get_airplanes(AIRPLANE_DATABASE))
    print_context(FILENAME, f"number of new invisible airplane: {len(airplanes)}")

    airport_in_zone = {}
    for airplane in airplanes:
        # we verify that the data can be interpreted, if not, it will be skipped
        try:
            airplane["latitude"] = float(airplane["latitude"])
            airplane["longitude"] = float(airplane["longitude"])
            airplane["altitude"] = float(airplane["altitude"])
            airplane["velocity"] = float(airplane["velocity"])
            airplane["heading"] = float(airplane["heading"])
        except:
            continue

        # create the zone on which the airplane can land
        airplane_zone = create_zone((airplane["latitude"], 
                                    airplane["longitude"]),
                                    airplane["altitude"],
                                    airplane["velocity"],
                                    airplane["heading"])
        for airport in airports:
            airport_coords = (float(airport["latitude"]), float(airport["longitude"]))
            if is_inside_polygon(airplane_zone, airport_coords):
                # add the airplane as key if it is not already
                if airplane["callname"] not in airport_in_zone.keys():
                    airport_in_zone[airplane["callname"]] = {"coords": {
                                                                "latitude": airplane["latitude"],
                                                                "longitude": airplane["longitude"]
                                                                },
                                                            "last_contact": airplane["last_contact"],
                                                            "airport": []
                                                            }
                # add the airport, if it is possible to the airplane to 
                # land there
                airport_in_zone[airplane["callname"]]["airport"].append({"regis" : airport["name"],
                                                                        "coords" : {
                                                                            "latitude": airport_coords[0],
                                                                            "longitude": airport_coords[1]
                                                                            }
                                                                        })
    if not previous_been_read(OUTPUT_FILE):
        with open(OUTPUT_FILE) as file:
            previous_airport_by_zone = json.loads(file.read())["data"]
            airport_in_zone = previous_airport_by_zone | airport_in_zone
    
    output_data = {
        "been_read": False, 
        "data": airport_in_zone
    }

    # write the possibilities in a JSON file
    with open(OUTPUT_FILE, "w+") as file:
        file.write(json.dumps(output_data, indent=2))

    print_context(FILENAME, "end of the routine")
    time.sleep(CYCLE_TIME)