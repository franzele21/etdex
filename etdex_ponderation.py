import jellyfish
import json
import time
import os
from os.path import exists
from functions import *
from datetime import datetime


# forces this program to be in the UTC timezone
os.environ["TZ"] = "UTC"
time.tzset()

LANDING_TIME = 15                       # in minutes
CORRELATION_APPROVAL_PROB = 0.5         # probability
LANDING_APPROVAL_PROB = 0.75            # probability
PPR_DELTA_TIME = 3                      # in hour
AFTN_DELTA_TIME = 20                    # in minutes
DELAY_BETWEEN_LANDINGS = 1              # in hour
DATABASE_PATH = "database.db"
PONDERATION_FILE = "ponderation.json"
POSSIBLE_LANDINGS_ADSB_FILE = "airport_by_zone.json"
PPR_FILE = "output_ppr.json"
AFTN_FILE = "data_traffic.json"
FILENAME = os.path.basename(__file__)
CYCLE_TIME = 900                        # in seconds

print_context(FILENAME, "initialization")


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


def find_probability(evidence1: tuple, evidence2: tuple) -> float:
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
    id_probability = 0
    time_probability = 0

    diff_letter = jellyfish.hamming_distance(evidence1[2], evidence2[2])
    id_probability = 1 - diff_letter / len(evidence1[2])

    delta_time = evidence1[6] - evidence2[6]
    delta_time = abs(delta_time / 60) + 1
    
    prob_calc = lambda lt, ts : (lt/ts) - (ts/(lt*2)) + ((lt/2)/lt)

    time_probability = prob_calc(LANDING_TIME, delta_time)
    time_probability = time_probability if time_probability < 1 else 1

    
    final_probability = (id_probability + time_probability) / 2

    return final_probability


def get_probability(evidence1: tuple, evidence2: tuple) -> float:
    """
Returns the probability of a landing, calculated with the ponderation
of it's sources

Parameters
----------
evidence1 : tuple
    First evidence to find the ponderation, yith the primary source on 
index 4 and the secondary source on index 5 (index begins at 0)
evidence2 : tuple
    Second evidence

Returns
-------
float
    Probability of a landing
    """
    coef1, coef2 = 1, 1
    if evidence1[5] in ponderation[evidence1[4]].keys():
        coef1 = ponderation[evidence1[4]][evidence1[5]]
    else:
        coef1 = ponderation[evidence1[4]]["default"]
    
    if evidence2[5] in ponderation[evidence2[4]].keys():
        coef2 = ponderation[evidence2[4]][evidence2[5]]
    else:
        coef2 = ponderation[evidence2[4]]["default"]

    prob = (evidence1[3] * coef1 + evidence2[3] * coef2) / (coef1 + coef2) 

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
                    if find_probability(evidence1, evidence2) > CORRELATION_APPROVAL_PROB:
                        evidence_correlation[evidence1].append(evidence2)
                        verified_evidences.append(evidence2)
    
    # here we will compute the probability of a landing, from a 
    # airplane to a airport, at a certain time 
    evidence_prob = []
    for evidence1 in evidence_correlation:
        probability = 0

        if len(evidence_correlation[evidence1]) > 0:
            for evidence2 in evidence_correlation[evidence1]:
                max_time = evidence1[6] if evidence1[6] > evidence2[6] else evidence2[6]
                
                probability += get_probability(evidence1, evidence2)
                
            probability = probability / len(evidence_correlation[evidence1]) / 100
            
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

while True:
    print_context(FILENAME ,"begin of the routine")

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
        all_ppr = []

    if exists(AFTN_FILE):
        with open(AFTN_FILE) as aftn_file:
            aftn_data = json.loads(aftn_file.read())
    else:
        aftn_data = {"been_read": True}

    conn = create_connection(DATABASE_PATH)

    # check if the table UNTREATED_TABLE exists
    table_exists = query(conn, "SELECT count(name) FROM sqlite_master WHERE type = 'table' AND name = 'UNTREATED_DATA';").fetchall()
    table_exists = table_exists[0][0]

    # if the count is equal to 0, we create both tables
    if table_exists == 0:
        print_context(FILENAME, "creating database table")
        query(conn, """
                    CREATE TABLE "UNTREATED_DATA" ( 
                        "udId" INTEGER NOT NULL,
                        "udAirport" TEXT NOT NULL, 
                        "udRegis" TEXT, 
                        "udProbability" REAL, 
                        "udSource" TEXT, 
                        "udSource2" TEXT,
                        "udTime" INTEGER, 
                        PRIMARY KEY("udId" AUTOINCREMENT) 
                    );
                    """)
        query(conn, """
                    CREATE TABLE "TREATED_DATA" (
                        "tdId" INTEGER NOT NULL,
                        "tdAirport" TEXT NOT NULL,
                        "tdAirplane" TEXT,
                        "tdTime" INTEGER,
                        "tdProb" REAL,
                        CONSTRAINT unique_combinaison UNIQUE (tdAirport, tdAirplane),
                        PRIMARY KEY("tdId" AUTOINCREMENT)
                    );
                    """)

    # add all airplanes coming from airTracker
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
            min_prob = 0.6
            weight_prob = 0.9
            tracked_airplane_probability = min_prob + (weight_prob / len(tracking[tracked_airplane]["airport"]))
            # it's a probability, so it has to be <=1
            tracked_airplane_probability = min(1, tracked_airplane_probability)


            tracked_airplane_time = tracking[tracked_airplane]["last_contact"]

            for possible_airport in tracking[tracked_airplane]["airport"]:
                query(conn, f"""
                                INSERT INTO "UNTREATED_DATA"
                                (udAirport, udRegis, udProbability, udSource, udTime)
                                VALUES ('{possible_airport["regis"]}', '{tracked_airplane}',
                                '{tracked_airplane_probability}', 'airTracker', 
                                '{tracked_airplane_time}');
                            """)

    # here is the data from UNTREATED_DATA going to be treated, this means
    # that it will find all evidences that could refer to the same landing,
    # (first part of evidence_probability) and then it will calculate the
    # probability of this landing, by weightening them, depending from 
    # they're sources (second part of evidence_probability)
    list_airports_name = query(conn, "SELECT udAirport FROM \"UNTREATED_DATA\" WHERE 1").fetchall()
    list_airports_name = push_id(list_airports_name)
    done_airport = []
    for airport_name in list_airports_name:
        if airport_name not in done_airport:
            done_airport.append(airport_name)
            
            evidences_by_airport = query(conn, f"SELECT * FROM \"UNTREATED_DATA\" WHERE udAirport='{airport_name}';")
            evidences_by_airport = evidences_by_airport.fetchall()

            for evidence in evidence_probability(evidences_by_airport):
                landing_exists =  query(conn, f"""
                                                    SELECT count(tdId)
                                                    FROM "TREATED_DATA"
                                                    WHERE tdAirport = '{evidence["airport"]}' 
                                                    AND tdAirplane = '{evidence["regis"]}';
                                                """).fetchone()[0]
                if landing_exists == 0:
                    query(conn, f"""
                                    INSERT INTO "TREATED_DATA"
                                    (tdAirport, tdAirplane, tdTime, tdProb)
                                    VALUES ('{evidence["airport"]}', '{evidence["regis"]}', 
                                    '{evidence["time"]}', '{evidence["prob"]}');
                                """)
                    query(conn, f"""
                                    DELETE FROM "UNTREATED_DATA"
                                    WHERE udAirport = '{evidence["airport"]}'
                                    AND udRegis = '{evidence["regis"]}'
                                """)

    # here are all the PPRs going to be checked, and if one has the same 
    # destination airport and same airplane. If one same landing has be 
    # found, the landing will have a 100% of probability 
    if not ppr_been_read:
        with open(PPR_FILE, "w+") as file:
            file.write(json.dumps({"been_read": True, "new_ppr": all_ppr}))
        for ppr_id in all_ppr:
            departing_airport = all_ppr[ppr_id]["departingTo"]
            present_airport = all_ppr[ppr_id]["airport"]
            airplane = all_ppr[ppr_id]["licenseNumber"]

            departure_time = all_ppr[ppr_id]["departure"]
            if "," in departure_time:
                departure_time = datetime.strptime(departure_time, "%a, %d %b %Y %H:%M:%S %Z")
            elif departure_time != "":
                departure_time = datetime.strptime(departure_time, "%a %b %d %Y %H:%M:%S %Z%z")
            else:
                continue
            departure_time = int(time.mktime(departure_time.timetuple()))
            
            arrival_time = all_ppr[ppr_id]["arrival"]
            if "," in arrival_time:
                arrival_time = datetime.strptime(arrival_time, "%a, %d %b %Y %H:%M:%S %Z")
            else:
                arrival_time = datetime.strptime(arrival_time, "%a %b %d %Y %H:%M:%S %Z%z")
            arrival_time = int(time.mktime(arrival_time.timetuple()))

            # searching for all landings, that could have been at the present
            # or the destination airport from the PPR
            landings = query(conn, f"""
                                        SELECT tdId, tdTime, tdAirport 
                                        FROM \"TREATED_DATA\" 
                                        WHERE tdAirport = '{departing_airport}'
                                        OR tdAirport = '{present_airport}'
                                        AND tdAirplane = '{airplane}';
                                    """)
            landings = landings.fetchall()

            for landing_data in landings:
                same_landing = False
                # first check if the ppr is usable for its destination
                if landing_data[2] == departing_airport:
                    time_limit = PPR_DELTA_TIME * 60 * 60
                    if landing_data[1] > departure_time and \
                            landing_data[1] < departure_time + time_limit:
                        same_landing = True

                # then check if the ppr is usable for its present airport
                if landing_data[2] == present_airport:
                    time_limit = PPR_DELTA_TIME * 60 * 60
                    min_time = arrival_time - (time_limit / 2)
                    max_time = arrival_time + (time_limit / 2)
                    
                    if landing_data[1] > min_time \
                            and landing_data[1] < max_time:
                        same_landing = True
                
                if same_landing:
                    query(conn, f"""
                                    UPDATE \"TREATED_DATA\"
                                    SET tdProb = '1.0'
                                    WHERE tdId = '{landing_data[0]}';
                                """)
            if len(landings) == 0:
                query(conn, f"""
                                INSERT INTO "TREATED_DATA"
                                (tdAirport, tdAirplane, tdTime, tdProb)
                                VALUES ('{departing_airport}', '{airplane}', 
                                '{departure_time}', '{ponderation["PPR"]["default"]/100.0}');
                            """)


    if not aftn_data["been_read"]:
        with open(AFTN_FILE, "w") as file:
            aftn_data["been_read"] = True
            file.write(json.dumps(aftn_data))

        for aftn_id in aftn_data["new_aftn"]:
            aftn = aftn_data["new_aftn"][aftn_id]
            aftn = get_aftn(aftn)

            landings = query(conn, f"""
                                        SELECT *
                                        FROM "TREATED_DATA"
                                        WHERE tdAirplane = '{aftn["airplane"]}'
                                        AND tdAirport = '{aftn["airport"]}';
                                    """)
            if not isinstance(landings, type(None))\
                    and len(landings.fetchall()) > 0:
                landings = landings.fetchall()
                for landing in landings:
                    if landing[3] - AFTN_DELTA_TIME < aftn["time"] \
                            and landing[3] + AFTN_DELTA_TIME > aftn["time"]:
                        query(conn, f"""
                                        UPDATE "TREATED_DATA"
                                        SET tdProb = '1.0'
                                        WHERE tdId = '{landing[0]}';
                                    """)
            else:
                query(conn, f"""
                                INSERT INTO "TREATED_DATA"
                                (tdAirport, tdAirplane, tdTime, tdProb)
                                VALUES ('{aftn["airport"]}', '{aftn["airplane"]}',
                                '{aftn["time"]}', '{ponderation["AFTN"]["default"]/100.0}');
                            """)

    # here, when an airplane appears two times, but not at the same airport
    # we will keep the most probable one. If there isn't a most probable
    # one we will keep them all
    all_landings = query(conn, "SELECT * FROM \"TREATED_DATA\" WHERE 1;")
    for landing in all_landings:
        landing_id = landing[0]         # get the id
        airplane_name = landing[2]      # get the registration
        max_prob = landing[4]           # get the probability
        most_prob_landing = landing_id
        prob_changed = False

        same_airplanes = query(conn, f"""
                                        SELECT * 
                                        FROM \"TREATED_DATA\" 
                                        WHERE tdAirplane = '{airplane_name}'
                                        AND tdId != '{landing_id}'
                                    """)
        if not isinstance(same_airplanes, type(None)):
            same_airplanes = same_airplanes.fetchall()

            for airplane in same_airplanes:
                if airplane[4] > max_prob:
                    max_prob = airplane[4]
                    most_prob_landing = airplane[0]
                    prob_changed = True
            if prob_changed:
                delta_landing = DELAY_BETWEEN_LANDINGS * 60 * 60
                landing_time = query(conn, f"""
                                                SELECT tdTime 
                                                FROM \"TREATED_DATA\" 
                                                WHERE tdId = '{most_prob_landing}';
                                            """).fetchone()[0]

                query(conn, f"""
                                DELETE FROM \"TREATED_DATA\"
                                WHERE tdId != '{most_prob_landing}'
                                AND tdAirplane = '{airplane_name}'
                                AND ABS(tdTime - '{landing_time}') <= '{delta_landing}';
                            """)
    conn.close()

    print_context(FILENAME, "end of the routine")
    time.sleep(CYCLE_TIME)