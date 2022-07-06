"""
This program is used to check the airplanes which comes from various 
APIs, stored in the database, and merge if one airplane appears two 
times on different API
"""

import time
from functions import *
import os

# final variable of how many minutes it takes to consider, that an
# airplane is really disappeared from the radars (in minutes)
DELAY_INVISIBLE = 5
# final variable, if an invisible airplane is older than that, 
# it will be deleted (in minutes)
DELAY_DELETE = 10
# minimum speed that an airplane can have in (m/s)
MIN_VELOCITY = 5
# path to the database
DATABASE_PATH = "airplane.db"
FILENAME = os.path.basename(__file__)
CYCLE_TIME = 100 

print_c = lambda text : print_context(FILENAME, text)

print_c("initialization")

while True:
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

    table_exists = query("SELECT count(name) FROM sqlite_master WHERE type = 'table' AND name = 'AIRPLANE';").fetchall()

    if table_exists[0][0] == 0:
        print_c("waiting for the AIRPLANE table to be created")
        time.sleep(CYCLE_TIME)
        continue


    source_list = query("SELECT DISTINCT apSource FROM \"AIRPLANE\" WHERE 1;").fetchall()
    source_list = [item[0] for item in source_list]

    # if the airplane hasn't given a signal after DELAY_INVISIBLE minutes,
    # it will registered as invisible
    airplanes = query("SELECT apRegis, apTime FROM \"AIRPLANE\" WHERE apInvisible = '0';")
    for airplane in airplanes:
        delta_time = int(time.time()) - airplane[1]
        if delta_time > int(DELAY_INVISIBLE * 60):
            wait_unlock_db(query, DATABASE_PATH, FILENAME)
            query(f"""
                    UPDATE "AIRPLANE"
                    SET apInvisible = '1',
                    apInvisibleTime = '{int(time.time())}'
                    WHERE apRegis = '{airplane[0]}';
                """)

    print_c("adding new airplanes")
    # add the airplane if it is really invisible in the INVISIBLE_AIRPLANE table
    airplanes = query("SELECT DISTINCT apRegis FROM \"AIRPLANE\" WHERE 1;").fetchall()
    for airplane in airplanes:
        wait_unlock_db(query, DATABASE_PATH, FILENAME)
        airplane_name = airplane[0]
        same_airplane = query(f"SELECT DISTINCT apInvisible FROM \"AIRPLANE\" WHERE apRegis = '{airplane_name}';")
        if same_airplane != False:
            same_airplane = [item[0] for item in same_airplane]
            if 0 in same_airplane:
                # with those deletion, we keep the only sources that can still track
                query(f"""
                        DELETE FROM \"AIRPLANE\"
                        WHERE apRegis = '{airplane_name}'
                        AND apInvisible = '1';
                    """)
            else:
                last_seen = query(f"""
                                    SELECT * FROM "AIRPLANE"
                                    WHERE apTime = (
                                        SELECT MAX(apTime) FROM "AIRPLANE"
                                        WHERE apRegis = '{airplane_name}'
                                    );
                                """)
                last_seen = last_seen.fetchone()
                if not isinstance(last_seen, type(None)):
                    is_in_db = query(f"SELECT count(apRegis) FROM \"INVISIBLE_AIRPLANE\" WHERE apRegis = '{last_seen[0]}';")
                    is_in_db = is_in_db.fetchall() if is_in_db else [[0]]

                    if is_in_db[0][0] == 0:
                        query(f"""
                                INSERT INTO "INVISIBLE_AIRPLANE"
                                VALUES ('{last_seen[0]}', '{last_seen[1]}', 
                                '{last_seen[2]}', '{last_seen[3]}', '{last_seen[4]}', 
                                '{last_seen[5]}', '{last_seen[6]}', '{last_seen[8]}')
                            """)

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
                is_in_db = query(f"SELECT count(apRegis) FROM \"INVISIBLE_AIRPLANE\" WHERE apRegis = '{last_seen[0]}';")
                is_in_db = is_in_db.fetchall() if not isinstance(is_in_db, type(None)) else [[0]]

                if is_in_db[0][0] == 0:
                    last_contact = last_seen[3] if last_seen[3] > 0 else int(time.time())
                    query(f"""
                            INSERT INTO "INVISIBLE_AIRPLANE"
                            VALUES ('{last_seen[0]}', '{last_seen[1]}', 
                            '{last_seen[2]}', '{last_contact}', '{last_seen[4]}', 
                            '{last_seen[5]}', '{last_seen[6]}', '{last_seen[8]}')
                        """)

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

    print_c("end of the routine")
    time.sleep(CYCLE_TIME)
