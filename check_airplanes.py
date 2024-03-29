"""
This program is used to check the airplanes which comes from various 
APIs, stored in the database, and merge if one airplane appears two 
times on different API
"""

import time
from functions import *
import os
import json

# final variable of how many minutes it takes to consider, that an
# airplane is really disappeared from the radars (in minutes)
DELAY_INVISIBLE = 5
# delay between 2 flight of an airplane (in minutes)
DELAY_FLIGHT = 15
# final variable, if an invisible airplane is older than that, 
# it will be deleted (in minutes)
DELAY_DELETE = 10
# minimum speed that an airplane can have in (m/s)
MIN_VELOCITY = 5
# path to the database
DATABASE_PATH = "airplane.db"
FILENAME = os.path.basename(__file__)
SAVE_FILE = "save_airplane.json"

print_c = lambda text : print_context(FILENAME, text)


print_c("begin of the routine")

conn = create_connection(DATABASE_PATH)
query = lambda query_ : query_to_bdd(conn, FILENAME, query_)

wait_unlock_db(query, DATABASE_PATH, FILENAME)


table_exists = query(f"SELECT count(name) FROM sqlite_master WHERE type = 'table' AND name = 'INVISIBLE_AIRPLANE';").fetchall()

if table_exists[0][0] == 0:
    # we have the basic parameters of a airplane (registration 
    # name, coordinate, altitude, speed and heading) qnd three more
    # aspects : when it was last seen, if it is invisible, and since
    # when it's invisible
    query("""
            CREATE TABLE "INVISIBLE_AIRPLANE" ( 
                "apRegis" TEXT NOT NULL, 
                "apLatitude" REAL, 
                "apLongitude" REAL, 
                "apAltitude" REAL, 
                "apTime" INTEGER,
                "apVelocity" REAL,
                "apHeading" REAL,
                "apInvisibleTime" INTEGER,
                CONSTRAINT unique_direction UNIQUE (apRegis),
                PRIMARY KEY ("apRegis") );
        """)

# if this program is executed before an get_airplane
table_exists = query("SELECT count(name) FROM sqlite_master WHERE type = 'table' AND name = 'AIRPLANE';").fetchall()

if table_exists[0][0] == 0:
    print_c("waiting for the AIRPLANE table to be created")
    exit()
    


source_list = query("SELECT DISTINCT apSource FROM \"AIRPLANE\" WHERE 1;").fetchall()
source_list = [item[0] for item in source_list]

# if the airplane hasn't given a signal after DELAY_INVISIBLE minutes,
# it will registered as invisible
airplanes = query("SELECT apRegis, apTime, apSource FROM \"AIRPLANE\" WHERE apInvisible = '0';")
for airplane in airplanes:
    delta_time = int(time.time()) - airplane[1]
    if delta_time > int(DELAY_INVISIBLE * 60):
        wait_unlock_db(query, DATABASE_PATH, FILENAME)
        query(f"""
                UPDATE "AIRPLANE"
                SET apInvisible = '1',
                apInvisibleTime = '{int(time.time())}'
                WHERE apRegis = '{airplane[0]}'
                AND apSource = '{airplane[2]}';
            """)

save_data_list = []

print_c("adding new airplanes")
# add the airplane if it is really invisible in the INVISIBLE_AIRPLANE table
airplanes = query("SELECT DISTINCT apRegis FROM \"AIRPLANE\" WHERE 1;").fetchall()
for airplane in airplanes:
    wait_unlock_db(query, DATABASE_PATH, FILENAME)
    airplane_name = airplane[0]
    # for a registration name, we see the different invisible status
    same_airplane = query(f"SELECT DISTINCT apInvisible FROM \"AIRPLANE\" WHERE apRegis = '{airplane_name}';")
    if same_airplane:
        same_airplane = [item[0] for item in same_airplane]
        if 0 in same_airplane:
            # 0 = still tracking, so there is at least one tracking service that
            # still detects the airplane.
            # with those deletion, we keep the only sources that can still track
            query(f"""
                    DELETE FROM \"AIRPLANE\"
                    WHERE apRegis = '{airplane_name}'
                    AND apInvisible = '1';
                """)
        else:
            # None of the tracking services can track this airplane, so it is
            # really invisible
            last_seen = query(f"""
                                SELECT * FROM "AIRPLANE"
                                WHERE apTime = (
                                    SELECT MAX(apTime) FROM "AIRPLANE"
                                    WHERE apRegis = '{airplane_name}'
                                );
                            """)
            last_seen = last_seen.fetchone()
            if not isinstance(last_seen, type(None)):
                is_in_db = query(f"""
                                    SELECT count(apRegis) 
                                    FROM \"INVISIBLE_AIRPLANE\" 
                                    WHERE apRegis = '{last_seen[0]}'
                                    AND apTime BETWEEN '{last_seen[4] - DELAY_FLIGHT * 60}'
                                                    AND '{last_seen[4] + DELAY_FLIGHT * 60}';
                                """)
                is_in_db = is_in_db.fetchall() if is_in_db else [[0]]

                if is_in_db[0][0] == 0:
                    query(f"""
                            INSERT INTO "INVISIBLE_AIRPLANE"
                            VALUES ('{last_seen[0]}', '{last_seen[1]}', 
                            '{last_seen[2]}', '{last_seen[3]}', '{last_seen[4]}', 
                            '{last_seen[5]}', '{last_seen[6]}', '{last_seen[8]}')
                        """)
                    save_data_list.append({"regis": last_seen[0], 
                                            "coords": {
                                                "latitude": last_seen[1], 
                                                "longitude": last_seen[2]
                                            }, 
                                            "altitude": last_seen[3], 
                                            "time": last_seen[4], 
                                            "velocity": last_seen[5], 
                                            "heading": last_seen[6], 
                                            "source": last_seen[9]
                                        })

                query(f"""
                        DELETE FROM \"AIRPLANE\"
                        WHERE apRegis = '{last_seen[0]}';
                    """)

# add airplanes that are too slow (= on the ground)
airplanes = query(f"SELECT DISTINCT apRegis FROM \"AIRPLANE\" WHERE apVelocity < '{MIN_VELOCITY}';").fetchall()
for airplane in airplanes:
    wait_unlock_db(query, DATABASE_PATH, FILENAME)
    airplane = airplane[0]
    same_airplanes_speed = query(f"SELECT apVelocity FROM \"AIRPLANE\" WHERE apRegis = '{airplane}';").fetchall()
    
    is_slow = all([speed[0] < MIN_VELOCITY for speed in same_airplanes_speed])
    
    if is_slow:
        last_seen = query(f"""
                            SELECT * FROM "AIRPLANE"
                            WHERE apTime = (
                                SELECT MAX(apTime) FROM "AIRPLANE"
                                WHERE apRegis = '{airplane}'
                            );
                        """)
        last_seen = last_seen.fetchone()
        if not isinstance(last_seen, type(None)):
            is_in_db = query(f"""
                                SELECT count(apRegis) 
                                FROM \"INVISIBLE_AIRPLANE\" 
                                WHERE apRegis = '{last_seen[0]}'
                                AND apTime BETWEEN '{last_seen[4] - DELAY_FLIGHT * 60}'
                                                AND '{last_seen[4] + DELAY_FLIGHT * 60}';
                            """)
            is_in_db = is_in_db.fetchall() if not isinstance(is_in_db, type(None)) else [[0]]

            if is_in_db[0][0] == 0:
                last_contact = last_seen[3] if last_seen[3] > 0 else int(time.time())
                query(f"""
                        INSERT INTO "INVISIBLE_AIRPLANE"
                        VALUES ('{last_seen[0]}', '{last_seen[1]}', 
                        '{last_seen[2]}', '{last_contact}', '{last_seen[4]}', 
                        '{last_seen[5]}', '{last_seen[6]}', '{last_seen[8]}')
                    """)
                save_data_list.append({"regis": last_seen[0], 
                                        "coords": {
                                            "latitude": last_seen[1], 
                                            "longitude": last_seen[2]
                                        }, 
                                        "altitude": last_seen[3], 
                                        "time": last_seen[4], 
                                        "velocity": last_seen[5], 
                                        "heading": last_seen[6], 
                                        "source": last_seen[9]
                                        })

            query(f"""
                    DELETE FROM \"AIRPLANE\"
                    WHERE apRegis = '{last_seen[0]}';
                """)

# delete invisible airplanes that are too old 
airplanes = query("SELECT apRegis, apInvisibleTime FROM \"AIRPLANE\" WHERE apInvisible = '1';")
for airplane in airplanes:
    airplane_name, airplane_time = airplane
    if int(time.time()) > (airplane_time + DELAY_DELETE * 60):
        query(f"""
                DELETE FROM "AIRPLANE"
                WHERE apRegis = '{airplane_name}';
            """)

conn.close()

try:
    with open(SAVE_FILE) as file:
        content = json.loads(file.read())
    last_id = max([int(x) for x in content.keys()])
    last_id += 1
except (FileNotFoundError, json.decoder.JSONDecodeError):
    file = open(SAVE_FILE, "w+")
    content = {}
    last_id = 0
    
new_content = {str(last_id + i): item for i, item in enumerate(save_data_list)}

new_content = content | new_content

with open(SAVE_FILE, "w+") as file:
    file.write(json.dumps(new_content, indent=2))

print_c("end of the routine")
