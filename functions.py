"""
Here you can find functions that are used in many other program
"""

import sqlite3
from sqlite3 import Error
from datetime import datetime
import os

def create_connection(path: str, check_same_thread :bool=True) -> sqlite3.Connection: #|None
    """
Establish the connection with the database

Parameters
----------
path : str
    Path to the database
check_same_thread : bool
    activate the check_same_thread of the connection (can be disabled 
by putting it as False)

Returns
-------
sqlite3.Connection or None
    Connection to the database or None
    """
    connection = None
    try:
        connection = sqlite3.connect(path, check_same_thread=check_same_thread)
    except Error as e:
        print(f"The error '{e}' occurred")

    return connection

def query(connection: sqlite3.Connection , query_: str) -> sqlite3.Cursor: #|None
    """
Makes a query to a database

Parameters
----------
connection : sqlite3.Connection
    Connection to the database on wich the query has to be made
query_ : str
    Query to be done on the database

Returns
-------
sqlite3.Cursor or None
    Response (if any) from the database of the query
    """    
    cursor = connection.cursor()
    try:
        cursor.execute(query_)
        connection.commit()
        return cursor
    except Error as e:
        print(f"Error: '{e}'")
        return False

class bcolors:
    reset="\033[0m"       # Text Reset

    get_airplane = "\033[1;94m"     # blue
    check_airplane = "\033[1;92m"   # green
    etdex_ponderation = "\033[1;90m\033[47m" # bold intense black + white background
    get_ppr = "\033[1;95m"          # purple
    get_aftn_by_id = "\033[1;93m"         # yellow
    get_airport_by_zone = "\033[1;36m"      # cyan


def print_context(file: str, message: str):
    file = file [:-3]
    file_class = file[:11] if "get_airplane" in file else file
    print(f"{datetime.now().strftime('%H:%M:%S')} | {getattr(bcolors, file_class)}{file}{bcolors.reset}:\t\t{message}")
