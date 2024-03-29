"""
This program calculate the probabilty of a landing, with different 
sources
"""

import jellyfish
import json
import time
import os
import requests
from math import isclose
from geopy import distance
from os.path import exists
from functions import *
from datetime import datetime


# forces this program to be in the UTC timezone
os.environ["TZ"] = "UTC"
time.tzset()

LANDING_TIME                = 15                            # in minutes
LANDING_DITANCE             = 2                             # in km
CORRELATION_APPROVAL_PROB   = 0.5                           # probability
LANDING_APPROVAL_PROB       = 0.75                          # probability
PPR_DELTA_TIME              = 3                             # in hour
AFTN_DELTA_TIME             = 20                            # in minutes
DELAY_BETWEEN_LANDINGS      = 1                             # in hour
BONUS_WEIGHT                = 5                             # probability
DATABASE_PATH               = "database.db"                 # output file
SEND_LANDING_PROGRAM        = "send_db.py"                  # program that sends the landing
PONDERATION_FILE            = "ponderation.json"
POSSIBLE_LANDINGS_ADSB_FILE = "airport_by_zone.json"
PPR_FILE                    = "output_ppr.json"
AFTN_FILE                   = "data_traffic.json"
AIRPORT_AUTH_PATH           = "auth_avdb.json"
FILENAME                    = os.path.basename(__file__)    # name of this file
CYCLE_TIME                  = 900                           # in seconds
TABLES                      = [
                                {
                                    "table_name": "TREATED_DATA", 
                                    "id_name": "tdId",
                                    "airplane_name": "tdAirplane",
                                    "airport_name": "tdAirport",
                                    "time_name": "tdTime"
                                }, {
                                    "table_name": "UNTREATED_DATA",
                                    "id_name": "udId",
                                    "airplane_name": "udRegis",
                                    "airport_name": "udAirport",
                                    "time_name": "udTime"
                                }
                            ]                          

print_c = lambda text : print_context(FILENAME, text)

print_c("initialization")


def push_id(list_: list) -> list:
    """
Used to push all tuple to the front, so the list isn't 
two-dimensionnal, but one-dimensionnal. The list can have this form:
[(x), (y), (z)] and will be formated to be [x, y, z]

Parameters
----------
list_ : list
    List of multiple tuple

Returns
-------
list
    One-dimensionnal list
    """
    final_list = []
    for item in list_:
        for item2 in item:
            final_list.append(item2)
    return final_list


def prob_same_landing(evidence1: tuple, evidence2: tuple) -> float:
    """
Used to see if the two evidence could refer to the same landing. 
For example, if an evidence points that an airplane x1 landed at airport 
y1, and an another evidence said that an airplane x2 landed at airport
y2, with x1 ≈ x2 and y1 ≈ y2 and the same landings in a same period of
time, the probability of those two evidences would be high

Parameters
----------
evidence1 : tuple
    One of the evidence to be compared to the other, to see if there 
is a correlation between the two. The form of this tuple is (index, 
airport, probability, source, source2, time)
evidence2 : tuple
    The other evidence, with the same form as the first evidence

Returns
-------
float
    Correlation between evidence1 and evidence2 
    """
    ev1, ev2 = evidence1, evidence2

    id_probability = 0
    time_probability = 0
    distance_probability = 0

    # first we calculate if the name could be the same
    diff_letter = jellyfish.hamming_distance(ev1[2], ev2[2])
    id_probability = 1 - (diff_letter / len(ev1[2])) * 1.5

    # then if its in the same time span
    delta_time = ev1[6] - ev2[6]
    delta_time = abs(delta_time / 60) + 1
    
    prob_calc = lambda lt, ts : min(1, (lt/ts) - (ts/(lt*2)) + ((lt/2)/lt))

    time_probability = prob_calc(LANDING_TIME, delta_time)

    # finally if it's in the same zone
    if (not isinstance(ev1[7], type(None)) or ev1[7] != 0) \
            and (not isinstance(ev1[8], type(None)) or ev1[8] != 0) \
            and (not isinstance(ev2[7], type(None)) or ev2[7] != 0) \
            and (not isinstance(ev2[8], type(None)) or ev2[8] != 0):

        prob_calc = lambda ld, ds : (ld/ds) - (ds/ld) + (ds/1)/ds

        evidence_distance = distance.distance((ev1[7], ev1[8]), (ev2[7], ev2[8])).km + 0.1
        distance_probability = min(1, prob_calc(LANDING_DITANCE, evidence_distance))
    else:
        distance_probability = max(0, (id_probability + time_probability) / 2)
    
    final_probability = max(0, (id_probability + time_probability + distance_probability) / 3)

    return final_probability


def get_probability(evidence: tuple) -> float:
    """
Returns the probability of a landing, calculated with the ponderation
of it's sources

Parameters
----------
evidence1 : tuple
    First evidence to find the ponderation, yith the primary source on 
index 4 and the secondary source on index 5 (index begins at 0)

Returns
-------
float
    Probability of a landing
    """
    coef = 1
    if evidence[5] in ponderation[evidence[4]].keys():
        coef = ponderation[evidence[4]][evidence[5]]
    else:
        coef = ponderation[evidence[4]]["default"]
    
    prob = evidence[3] * (coef/100) 

    return prob


def evidence_probability(evidences: list) -> list:
    """
Gives a list of all possible landings

Parameters
----------
evidences : list
    List of all evidences, that points to the same airport

Returns
-------
list
    List of all possible landings (with which airport, airplane, when
it was, and i's probability)
    """
    verified_evidences = []
    evidence_correlation = {}
    for evidence1 in evidences:
        if evidence1 not in verified_evidences:
            evidence_correlation[evidence1] = []
            verified_evidences.append(evidence1)

            for evidence2 in evidences:
                if evidence1[0] != evidence2[0] and evidence2 not in verified_evidences:

                    # here we will see if the two evidence could refer
                    #  to the same landing
                    if prob_same_landing(evidence1, evidence2) > CORRELATION_APPROVAL_PROB:
                        evidence_correlation[evidence1].append(evidence2)
                        verified_evidences.append(evidence2)
    
    # here we will compute the probability of a landing, from a 
    # airplane to a airport, at a certain time 
    evidence_prob = []
    for evidence1 in evidence_correlation:
        probability = 0

        if len(evidence_correlation[evidence1]) > 0:
            probability += get_probability(evidence1)
            for evidence2 in evidence_correlation[evidence1]:
                max_time = evidence1[6] if evidence1[6] > evidence2[6] else evidence2[6]
                
                probability += get_probability(evidence2)
                
            probability = probability / (len(evidence_correlation[evidence1])+1)
            probability += BONUS_WEIGHT * len(evidence_correlation[evidence1]) / 100
            probability = min(1, probability)
            
        else:
            if evidence1[5] in ponderation[evidence1[4]].keys():
                coef = ponderation[evidence1[4]][evidence1[5]]
            else:
                coef = ponderation[evidence1[4]]["default"]
            probability = evidence1[3] * coef / 100
            max_time = evidence1[6]

        if probability > LANDING_APPROVAL_PROB:
            evidence_prob.append({"airport": evidence1[1], 
                                    "regis": evidence1[2],
                                    "time": max_time,
                                    "prob": probability})

    return evidence_prob


def get_aftn(aftn_message: dict) -> dict:
    """
Transform the aftn message in a more readable message, with only the 
parts that are interesting to us (date, callsign and airport)

Parameters
----------
aftn_message : dict
    the original aftn message

Return
------
dict
    the aftn, with only the data that interests us

Note
----
This function was created after some changes in the return data of 
the aftn-message, because previously the messages was compatible with
the program, but now it needs to be translated.
    """
    # the FLIGHTDATE is the date of the movement, and TARR is the 
    # landing time
    aftn_time = aftn_message["FLIGHTDATE"] + "T" + aftn_message["TARR"]
    aftn_time = datetime.strptime(aftn_time, "%Y-%m-%dT%H:%M:%S")
    aftn_time = int(time.mktime(aftn_time.timetuple())) # we convert it to an unix timestamp
    
    return {
        "airplane": aftn_message["CALLSIGN"],
        "airport": aftn_message["AARR"],
        "time": aftn_time
    }


with open(PONDERATION_FILE) as ponderation_file:
    ponderation = json.loads(ponderation_file.read())

with open(AIRPORT_AUTH_PATH) as file:
    content = json.loads(file.read())
    user_avdb, password_avdb = content["user"], content["password"]


response = requests.get("https://avdb.aerops.com/public/airports", 
                        auth=(user_avdb, password_avdb))
airports = json.loads(response.text)["data"]

all_airports_name = [airport["name"] for airport in airports]


while True:
    print_c("begin of the routine")

    if exists(POSSIBLE_LANDINGS_ADSB_FILE):
        with open(POSSIBLE_LANDINGS_ADSB_FILE) as tracking_file:
            content = json.loads(tracking_file.read())
            tracking_been_read = content["been_read"]
            tracking = content["data"]
    else:
        tracking_been_read = True
        tracking = []

    if exists(PPR_FILE):
        with open(PPR_FILE) as ppr_file:
            content = json.loads(ppr_file.read())
            ppr_been_read = content["been_read"]
            all_ppr = content["new_ppr"]
    else:
        ppr_been_read = True
        all_ppr = {}

    if exists(AFTN_FILE):
        with open(AFTN_FILE) as aftn_file:
            aftn_data = json.loads(aftn_file.read())
    else:
        aftn_data = {"been_read": True}

    conn = create_connection(DATABASE_PATH)
    query = lambda query_ : query_to_bdd(conn, FILENAME, query_)
    
    # check if the table UNTREATED_TABLE exists
    table_exists = query("SELECT count(name) FROM sqlite_master WHERE type = 'table' AND name = 'UNTREATED_DATA';").fetchall()
    table_exists = table_exists[0][0]

    # if the count is equal to 0, we create both tables
    if table_exists == 0:
        print_c("creating database table")
        query("""
                CREATE TABLE "UNTREATED_DATA" ( 
                    "udId" INTEGER NOT NULL,
                    "udAirport" TEXT NOT NULL, 
                    "udRegis" TEXT, 
                    "udProbability" REAL, 
                    "udSource" TEXT, 
                    "udSource2" TEXT,
                    "udTime" INTEGER, 
                    "udLatitude" REAL,
                    "udLongitude" REAL,
                    PRIMARY KEY("udId" AUTOINCREMENT) 
                );
                """)
        query("""
                CREATE TABLE "TREATED_DATA" (
                    "tdId" INTEGER NOT NULL,
                    "tdAirport" TEXT NOT NULL,
                    "tdAirplane" TEXT,
                    "tdTime" INTEGER,
                    "tdProb" REAL,
                    "tdSent" INTEGER,
                    CONSTRAINT unique_combinaison UNIQUE (tdAirport, tdAirplane),
                    PRIMARY KEY("tdId" AUTOINCREMENT)
                );
                """)

    # add all airplanes coming from airTracker
    print_c("Adding new airplanes...")
    if not tracking_been_read: 
        with open(POSSIBLE_LANDINGS_ADSB_FILE, "w+") as file:
            file.write(json.dumps({"been_read": True, "data": tracking}))
        
        for tracked_airplane in tracking:
            # the probability is calculated, that it will always be more than 
            # min_prob, and the more weight_prob is high, the more the final
            # probability will be higher. Those number were choosen, so it will
            # accept from 1 to 3 different airports, but if the numbers of 
            # airports is greater than 3, the probability will not be high 
            # enough, and will not be accepted by evidence_probability
            min_prob = 0.5
            weight_prob = 0.8
            tracked_airplane_probability = min_prob + (weight_prob / len(tracking[tracked_airplane]["airport"]))
            # it's a probability, so it has to be <=1
            tracked_airplane_probability = min(1, tracked_airplane_probability)


            tracked_airplane_time = tracking[tracked_airplane]["last_contact"]

            for possible_airport in tracking[tracked_airplane]["airport"]:
                if tracked_airplane_probability > LANDING_APPROVAL_PROB:
                    query(f"""
                                INSERT INTO "UNTREATED_DATA"
                                (udAirport, udRegis, udProbability, udSource, udTime, udLatitude, udLongitude)
                                VALUES ('{possible_airport["regis"]}', '{tracked_airplane}',
                                '{tracked_airplane_probability}', 'airTracker', 
                                '{tracked_airplane_time}', 
                                '{tracking[tracked_airplane]["coords"]["latitude"]}',
                                '{tracking[tracked_airplane]["coords"]["longitude"]}');
                            """)

    # here are all the PPRs going to be checked, and if one has the same 
    # destination airport and same airplane. If one same landing has be 
    # found, the landing will have a 100% of probability
    print_c("Adding and treating the PPR...")
    number_ppr = query("SELECT COUNT(udId) FROM \"UNTREATED_DATA\" WHERE udSource = \"PPR\";").fetchone()[0]
    if not ppr_been_read or number_ppr > 0:
        with open(PPR_FILE, "w+") as file:
            file.write(json.dumps({"been_read": True, "new_ppr": all_ppr}))

        # if the ppr has been read, we reset the variable
        all_ppr = {} if ppr_been_read else all_ppr

        # we take the last ppr
        max_id = [int(x) for x in all_ppr]
        if len(max_id) == 0:
            max_id = 0
        else:
            max_id = max(max_id)

        # we add the ppr that are already in the database
        ppr_in_db = query("""
                                SELECT * 
                                FROM "UNTREATED_DATA"
                                WHERE udSource = "PPR";
                            """).fetchall()
        older_ppr = {}
        for index, item in enumerate(ppr_in_db):
            older_ppr[str(max_id + index + 1)] = {
                "airport": item[1],
                "arrivingFrom": item[1],
                "departingTo": item[1],
                "licenseNumber": item[2],
                "arrival": datetime.utcfromtimestamp(int(item[6])).strftime("%a, %d %b %Y %H:%M:%S UTC"),
                "departure": datetime.utcfromtimestamp(int(item[6])).strftime("%a, %d %b %Y %H:%M:%S UTC")
            }

        all_ppr = older_ppr | all_ppr

        for ppr_id in all_ppr:
            departing_airport = all_ppr[ppr_id]["departingTo"]
            present_airport = all_ppr[ppr_id]["airport"]
            airplane = all_ppr[ppr_id]["licenseNumber"]

            departure_time = all_ppr[ppr_id]["departure"]
            if "," in departure_time:
                departure_time = datetime.strptime(departure_time, "%a, %d %b %Y %H:%M:%S %Z")
            elif departure_time != "":
                departure_time = datetime.strptime(departure_time, "%a %b %d %Y %H:%M:%S %Z%z")
            departure_time = int(time.mktime(departure_time.timetuple()))
            
            arrival_time = all_ppr[ppr_id]["arrival"]
            if "," in arrival_time:
                arrival_time = datetime.strptime(arrival_time, "%a, %d %b %Y %H:%M:%S %Z")
            elif arrival_time != "":
                arrival_time = datetime.strptime(arrival_time, "%a %b %d %Y %H:%M:%S %Z%z")
            else:
                continue
            arrival_time = int(time.mktime(arrival_time.timetuple()))

            # searching for all landings, that could have been at the present
            # or the destination airport from the PPR
            landings = query(f"""
                                SELECT udId, udTime, udAirport, udSource 
                                FROM \"UNTREATED_DATA\" 
                                WHERE (udAirport = '{departing_airport}'
                                OR udAirport = '{present_airport}')
                                AND udRegis = '{airplane}'
                                AND udSource != 'PPR';
                            """)
            landings = landings.fetchall()

            added = False
            for landing_data in landings:
                same_landing_departing = False
                same_landing_present = False
                

                # first check if the ppr is usable for its destination
                if landing_data[2] == departing_airport:
                    time_limit = PPR_DELTA_TIME * 60 * 60
                    if landing_data[1] > departure_time and \
                            landing_data[1] < departure_time + time_limit:
                        query(f"""
                                INSERT INTO "TREATED_DATA"
                                (tdAirport, tdAirplane, tdTime, tdProb, tdSent)
                                VALUES ('{departing_airport}', '{airplane}', 
                                '{landing_data[1]}', '1', '0');
                            """)
                        query(f"""
                                DELETE FROM "UNTREATED_DATA"
                                WHERE udAirport = '{departing_airport}'
                                AND udRegis = '{airplane}';
                            """)
                        added = True

                # then check if the ppr is usable for its present airport
                if landing_data[2] == present_airport:
                    time_limit = PPR_DELTA_TIME * 60 * 60
                    min_time = arrival_time - (time_limit / 2)
                    max_time = arrival_time + (time_limit / 2)
                    
                    if landing_data[1] > min_time \
                            and landing_data[1] < max_time:
                        query(f"""
                                INSERT INTO "TREATED_DATA"
                                (tdAirport, tdAirplane, tdTime, tdProb, tdSent)
                                VALUES ('{present_airport}', '{airplane}',
                                '{landing_data[1]}', '1', '0');
                            """)
                        query(f"""
                                DELETE FROM "UNTREATED_DATA"
                                WHERE udAirport = '{present_airport}'
                                AND udRegis = '{airplane}';
                            """)                        
                        added = True

            if not added and int(ppr_id) <= max_id:
                response = requests.get("https://avdb.aerops.com/public/airports", 
                                auth=(user_avdb, password_avdb))
                data = json.loads(response.text)["data"]

                if present_airport in all_airports_name and arrival_time > 0:
                    coords = [(x["latitude"], x["longitude"]) for x in data if x["name"] == present_airport]
                    coords = coords[0] if len(coords) > 0 else (None, None)

                    query(f"""
                            INSERT INTO "UNTREATED_DATA"
                            (udAirport, udRegis, udTime, udProbability, udSource, udLatitude, udLongitude)
                            VALUES ('{present_airport}', '{airplane}', 
                            '{arrival_time}', '1', 'PPR', '{coords[0]}', '{coords[1]}');
                        """)
                if departing_airport in all_airports_name and departure_time > 0:
                    coords = [(x["latitude"], x["longitude"]) for x in data if x["name"] == departing_airport]
                    coords = coords[0] if len(coords) > 0 else (None, None)

                    query(f"""
                            INSERT INTO "UNTREATED_DATA"
                            (udAirport, udRegis, udTime, udProbability, udSource, udLatitude, udLongitude)
                            VALUES ('{departing_airport}', '{airplane}', 
                            '{departure_time}', '1', 'PPR', '{coords[0]}', '{coords[1]}');
                        """)
    
    print_c("Verify PPR on the treated data...")
    ppr_in_db = query("SELECT udAirport, udRegis, udTime FROM UNTREATED_DATA WHERE udSource = 'PPR';").fetchall()
    for ppr in ppr_in_db:
        has_movement = query(f"""
                                SELECT COUNT(tdId)
                                FROM "TREATED_DATA"
                                WHERE tdAirport = '{ppr[0]}'
                                AND tdAirplane = '{ppr[1]}'
                                AND tdTime > '{ppr[2] - PPR_DELTA_TIME * 60 * 60}'
                                AND tdTime < '{ppr[2] + PPR_DELTA_TIME * 60 * 60}';
                            """).fetchone()
        if not isinstance(has_movement, type(None)):
            query(f"""
                    UPDATE \"TREATED_DATA\"
                    SET tdProb = '1' 
                    WHERE tdAirport = '{ppr[0]}'
                    AND tdAirplane = '{ppr[1]}'
                    AND tdTime > '{ppr[2] - PPR_DELTA_TIME * 60 * 60}'
                    AND tdTime < '{ppr[2] + PPR_DELTA_TIME * 60 * 60}';
                """)
            query(f"""
                    DELETE FROM "UNTREATED_DATA"
                    WHERE udAirport = '{ppr[0]}'
                    AND udRegis = '{ppr[1]}'
                    and udTime = '{ppr[2]}';
                """)

    # here is the data from UNTREATED_DATA going to be treated, this means
    # that it will find all evidences that could refer to the same landing,
    # (first part of evidence_probability) and then it will calculate the
    # probability of this landing, by weightening them, depending from 
    # they're sources (second part of evidence_probability)
    print_c("Treat the data...")
    list_airports_name = query("SELECT udAirport FROM \"UNTREATED_DATA\" WHERE 1").fetchall()
    list_airports_name = push_id(list_airports_name)
    done_airport = []
    for airport_name in list_airports_name:
        if airport_name not in done_airport:
            done_airport.append(airport_name)
            
            evidences_by_airport = query(f"SELECT * FROM \"UNTREATED_DATA\" WHERE udAirport='{airport_name}';")
            evidences_by_airport = evidences_by_airport.fetchall()

            for evidence in evidence_probability(evidences_by_airport):
                landing_exists =  query(f"""
                                            SELECT count(tdId)
                                            FROM "TREATED_DATA"
                                            WHERE tdAirport = '{evidence["airport"]}' 
                                            AND tdAirplane = '{evidence["regis"]}'
                                            AND tdTime BETWEEN '{evidence["time"] - DELAY_BETWEEN_LANDINGS * 60 *60}'
                                                            AND '{evidence["time"] + DELAY_BETWEEN_LANDINGS * 60 *60}';
                                        """).fetchone()[0]
                if landing_exists == 0:
                    query(f"""
                            INSERT INTO "TREATED_DATA"
                            (tdAirport, tdAirplane, tdTime, tdProb, tdSent)
                            VALUES ('{evidence["airport"]}', '{evidence["regis"]}', 
                            '{evidence["time"]}', '{evidence["prob"]}', '0');
                        """)
                query(f"""
                        DELETE FROM "UNTREATED_DATA"
                        WHERE udAirport = '{evidence["airport"]}'
                        AND udRegis = '{evidence["regis"]}'
                        AND tdTime BETWEEN '{evidence["time"] - DELAY_BETWEEN_LANDINGS * 60 *60}'
                                    AND '{evidence["time"] + DELAY_BETWEEN_LANDINGS * 60 *60}';
                    """)


    print_c("Adding and treating the new AFTN messages...")
    if not aftn_data["been_read"]:
        with open(AFTN_FILE, "w") as file:
            aftn_data["been_read"] = True
            file.write(json.dumps(aftn_data))

        for aftn_id in aftn_data["new_aftn"]:
            aftn = aftn_data["new_aftn"][aftn_id]
            aftn = get_aftn(aftn)

            for table in TABLES:
                landing = query(f"""
                                    SELECT {table["id_name"]}
                                    FROM "{table["table_name"]}"
                                    WHERE {table["airplane_name"]} = '{aftn["airplane"]}'
                                    AND {table["airport_name"]} = '{aftn["airport"]}'
                                    AND {table["time_name"]} BETWEEN '{aftn["time"] - AFTN_DELTA_TIME * 60}' 
                                                                AND '{aftn["time"] + AFTN_DELTA_TIME * 60}';
                                """)
                landing = landing.fetchone()

                if not isinstance(landing, type(None)):
                    query(f"""
                        DELETE FROM "{table["table_name"]}"
                        WHERE {table["id_name"]} = {landing[0]};
                    """)
                    query(f"""
                        INSERT INTO "TREATED_DATA"
                        (tdAirport, tdAirplane, tdTime, tdProb, tdSent)
                        VALUES ('{aftn["airport"]}', '{aftn["airplane"]}',
                        '{aftn["time"]}', '1', '0');
                    """)

            else:
                # if there wasn't an occurence, it will be directly added
                query(f"""
                        INSERT INTO "TREATED_DATA"
                        (tdAirport, tdAirplane, tdTime, tdProb, tdSent)
                        VALUES ('{aftn["airport"]}', '{aftn["airplane"]}',
                        '{aftn["time"]}', '{ponderation["AFTN"]["default"]/100.0}', '0');
                    """)

    # here, when an airplane appears two times, but not at the same airport
    # we will keep the most probable one. If there isn't a most probable
    # one we will keep them all
    all_landings = query("SELECT DISTINCT tdAirplane FROM \"TREATED_DATA\" WHERE 1;")
    for airplane_name in push_id(all_landings.fetchall()):
        same_airplanes = query(f"""
                                    SELECT * 
                                    FROM \"TREATED_DATA\" 
                                    WHERE tdAirplane = '{airplane_name}';
                                """).fetchall()
        if len(same_airplanes) > 1:
            same_time_landing = [[same_airplanes[0]]]
            for landing in same_airplanes[1:]:
                added = False
                for same_landings in same_time_landing:
                    mean_time = 0
                    for tmp_landing in same_landings:
                        mean_time += tmp_landing[3]
                    mean_time /= len(same_landings)

                    if abs(landing[3] - mean_time) < DELAY_BETWEEN_LANDINGS * 60 * 60:
                        added = True
                        same_landings.append(landing)
                if not added:
                    same_time_landing.append([landing])
            
            new_landing = {}
            for group in same_time_landing:
                most_prob_landings = [group[0]]
                for landing in group[1:]:
                    if isclose(landing[4], most_prob_landings[0][4]):
                        most_prob_landings.append(landing)
                    elif landing[4] > most_prob_landings[0][4]:
                        most_prob_landings = [landing]
                mean_time = 0
                for landing in most_prob_landings:
                    mean_time += landing[3]
                mean_time /= len(most_prob_landings)
                new_landing[mean_time] = most_prob_landings
            
            for landing_time in new_landing:
                query(f"""
                        DELETE FROM TREATED_DATA
                        WHERE tdAirplane = '{airplane_name}'
                        AND tdTime BETWEEN '{landing_time - DELAY_BETWEEN_LANDINGS * 60 *60}'
                                        AND '{landing_time + DELAY_BETWEEN_LANDINGS * 60 *60}';
                    """)
            for landing in new_landing.values():
                landing = landing[0]
                query(f"""
                        INSERT INTO "TREATED_DATA"
                        (tdAirport, tdAirplane, tdTime, tdProb, tdSent)
                        VALUES ('{landing[1]}', '{landing[2]}', '{landing[3]}', '{landing[4]}', '0');
                    """)

    conn.close()

    # here we call the program that sends the landings to the API
    print_c("Sending data to the AVDB API...")
    # os.system(f"python3 {SEND_LANDING_PROGRAM}")

    print_c("end of the routine")
    time.sleep(CYCLE_TIME)