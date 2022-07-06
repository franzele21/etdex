"""
Here you can find functions that are used in many other program
"""

import sqlite3
import time
from sqlite3 import Error
from datetime import datetime

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

def query_to_bdd(connection: sqlite3.Connection, file: str, query_: str) -> sqlite3.Cursor: #|None
    """
Makes a query to a database

Parameters
----------
connection : sqlite3.Connection
    Connection to the database on wich the query has to be made
file : str
    FIle which makes the query
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
    except sqlite3.OperationalError:
        print_context(file, "Error: the database is locked", True)
        return "locked"
    except Error as e:
        print_context(file, f"Error: '{e}'", True)
        return False

class bcolors:
    reset="\033[0m"       # Text Reset

    get_airplane = "\033[1;94m"                 # blue
    check_airplanes = "\033[1;92m"              # green
    etdex_ponderation = "\033[1;90m\033[47m"    # bold intense black + white background
    get_ppr = "\033[1;95m"                      # purple
    get_aftn_by_id = "\033[1;93m"               # yellow
    get_airport_by_zone = "\033[1;36m"          # cyan
    error = "\u001b[31m"                       # red
    default = "\033[1;37m"


def print_context(file: str, message: str, is_error: bool=False) -> None:
    """
Use to print context from a file, with color for each file
The print format is :
<time> | <file>: <message>

Parameters
----------
file : str
    The file that needs to print context
message : str
    The content that needs to be printed
is_error : bool
    If true, the message is an error message

Returns
-------
None
    Print a formated message
    """
    file = file [:-3]
    file_class = file[:12] if "get_airplane" in file else file

    if file_class not in dir(bcolors): file_class = "default"

    output_time = datetime.now().strftime('%H:%M:%S')
    color = getattr(bcolors, file_class)
    output_file = f"{color}{file}{bcolors.reset}:"
    output_file = output_file.ljust(45)

    if is_error:
        message = f"{bcolors.error}{message}"

    print(f"{output_time} | {output_file} {message}")


def wait_unlock_db(query, DATABASE_PATH: str, FILENAME: str, SOURCE: str="") -> None:
    """
Will stop the program, if the database is locked

Parameters
----------
query : function
    The function to make querie on a database
DATABASE_PATH : str
    The path/name of the database
FILENAME : str
    The file that execute this function
SOURCE : str
    For the "get_aiplane" programms, the name of the source
    """
    db_status = query(f"""
                    INSERT INTO "AIRPLANE" 
                    VALUES ("{FILENAME}_", "", "", "", "", "", "", "", "", "{SOURCE}");
                """)

    query(f"DELETE FROM \"AIRPLANE\" WHERE apRegis = \"{FILENAME}_\" AND apSource = \"{SOURCE}\";")
    while db_status == "locked":
        print_context(FILENAME, f"waiting for the {DATABASE_PATH} database to be unlocked")
        
        time.sleep(5)
        db_status = query(f"""
                            INSERT INTO "AIRPLANE" 
                            VALUES ("{FILENAME}_", "", "", "", "", "", "", "", "", "{SOURCE}");
                        """)
        query(f"DELETE FROM \"AIRPLANE\" WHERE apRegis = \"{FILENAME}_\" AND apSource = \"{SOURCE}\";")