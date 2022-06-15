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
from functions import *
from airplane_zone import create_zone
from in_polygon import is_inside_polygon

# database, where the airplanes are registered
AIRPLANE_DATABASE = "airplane.db"
# the path of the output file
OUTPUT_FILE = "airport_by_zone.json"

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
Used to get the airplanes to analyse

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
    airplanes = query(conn, "SELECT * FROM \"INVISBLE_AIRPLANE\" WHERE 1;").fetchall()

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



# get all airports, and keep just the ones that have their coordinates
response = requests.get("https://avdb.aerops.com/public/airports", auth=("ETDEX", "ijhf93**h&2eg2ge"))
airports = json.loads(response.text)["data"]
airports = filter_airports(airports)

while True:
    # get the airplanes that are inivisble by the ADS-B system
    airplanes = format_airplanes(get_airplanes(AIRPLANE_DATABASE))

    airport_in_zone = {}
    for airplane in airplanes:
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

    # write the possibilities in a JSON file
    with open(OUTPUT_FILE, "w+") as file:
        file.write(json.dumps(airport_in_zone, indent=2))

    time.sleep(180)