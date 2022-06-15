"""
This program is used to check the airplanes which comes from various 
APIs, stored in the database, and merge if one airplane appears two 
times on different API
"""

import time
from functions import *

# final variable of how many minutes it takes to consider, that an
# airplane is really disappeared from the radars (in minutes)
DELAY_INVISIBLE = 5
# final variable, if an invisible airplane is older than that, 
# it will be deleted (in minutes)
DELAY_DELETE = 10
# path to the database
DATABASE_PATH = "airplane.db"

while True:
    conn = create_connection(DATABASE_PATH)
    table_exists = query(conn, "SELECT count(name) FROM sqlite_master WHERE type = 'table' AND name = 'INVISIBLE_AIRPLANE';").fetchall()

    if table_exists[0][0] == 0:
        # we have the basic parameters of a airplane (registration 
        # name, coordinate, altitude, speed and heading) qnd three more
        # aspects : when it was last seen, if it is invisible, and since
        # when it's invisible
        query(conn, """
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


    source_list = query(conn, "SELECT DISTINCT apSource FROM \"AIRPLANE\" WHERE 1;").fetchall()
    source_list = [item[0] for item in source_list]

    # if the airplane hasn't given a signal after DELAY_INVISIBLE minutes,
    # it will registered as invisible
    airplanes = query(conn, "SELECT apRegis, apTime FROM \"AIRPLANE\" WHERE apInvisible = '0';")
    for airplane in airplanes:
        delta_time = int(time.time()) - airplane[1]
        if delta_time > int(DELAY_INVISIBLE * 60):
            query(conn, f"""
                            UPDATE "AIRPLANE"
                            SET apInvisible = '1',
                            apInvisibleTime = '{int(time.time())}'
                            WHERE apRegis = '{airplane[0]}';
                        """)

    # add the airplane if it is really invisible to the INVISBLE_AIRPLANE table
    airplanes = query(conn, "SELECT DISTINCT apRegis FROM \"AIRPLANE\" WHERE 1;").fetchall()
    for airplane in airplanes:
        airplane_name = airplane[0]
        same_airplane = query(conn, f"SELECT DISTINCT apInvisible FROM \"AIRPLANE\" WHERE apRegis = '{airplane_name}';")
        same_airplane = [item[0] for item in same_airplane]
        if 0 in same_airplane:
            query(conn, f"""
                            DELETE FROM \"AIRPLANE\"
                            WHERE apRegis = '{airplane_name}'
                            AND apInvisible = '1';
                        """)
        else:
            last_seen = query(conn, f"""
                                        SELECT * FROM "AIRPLANE"
                                        WHERE apTime = (
                                            SELECT MAX(apTime) FROM "AIRPLANE"
                                            WHERE apRegis = '{airplane_name}'
                                        );
                                    """)
            last_seen = last_seen.fetchone()
            is_in_db = query(conn, f"SELECT * FROM \"AIRPLANE\" WHERE APrEGIS = '{last_seen[0]}';")
            if isinstance(is_in_db, type(None)):
                query(conn, f"""
                                INSERT INTO "INVISIBLE_AIRPLANE"
                                VALUES ('{last_seen[0]}', '{last_seen[1]}', 
                                '{last_seen[2]}', '{last_seen[3]}', '{last_seen[4]}', 
                                '{last_seen[5]}', '{last_seen[6]}', '{last_seen[8]}')
                            """)

    # delete invisible airplanes that are too old 
    airplanes = query(conn, "SELECT apRegis, apInvisibleTime FROM \"AIRPLANE\" WHERE apInvisible = '1';")
    for airplane in airplanes:
        airplane_name, airplane_time = airplane
        if int(time.time()) > (airplane_time + DELAY_DELETE * 60):
            query(conn, f"""
                            DELETE FROM "AIRPLANE"
                            WHERE apRegis = '{airplane_name}';
                        """)

    conn.close()

    # it will pause 30 seconds, so we won't have any problem with the APIs
    time.sleep(30)
