import sqlite3
import os

from netaddr import IPAddress, IPNetwork

JSONLOG = "cowrie.json.2019-07-04"
CITY_LOCATIONS_CSV = "GeoLite2-City-Locations-en.csv"
GEOLITE2_CITY = "GeoLite2-City.mmdb"
GEOLITE2_ASN = "GeoLite2-ASN.mmdb"

COWRIE_LOG_DIR = "cowrie/"
DB_FILE = "db.sqlite"


def log_length(log):
    """ Gets the length of a logfile """
    i = 0
    with open(jsonlog) as f:
        for line in f:
            i += 1

    f.close()
    return i


def chunk(jsonlog, chunk_count):
    """ Divides a file into equal sized chunks. 
        Useful for dividing logs up for multiprocessing 
    """
    chunks = []

    length = log_length(jsonlog)
    chunk_size = length // chunk_count

    with open(jsonlog) as f:
        i = 0
        chunk = 0
        for line in f:
            chunks.append(line)
            if i == chunk_size:
                chunk += 1
                i = 0
            i += 1
    f.close()

    return chunks


def get_session_ids(logs):
    """ Parses a cowrie json log and gets all unique session IDs """

    sessions = {}
    for log in logs:
        with open(log) as f:
            for line in f:
                record = json.loads(line)
                session = record.get("session")
                if session not in sessions:
                    sessions[session] = ""
    return sessions


def connect_db(db_file):
    """ Connects to SQLite database """

    connection = None
    try:
        connection = sqlite3.connect(db_file)
    except (Exception, sqlite3.Error) as error:
        print(error)
    return connection


def init_db(conn):
    """ Creates all the necessary database table"""

    create_table = """CREATE TABLE IF NOT EXISTS attack_log(
                             src_ip text,
                             asn text,
                             timestamps text,
                             first_seen text,
                             last_seen text,
                             attack_count int,
                             country_code text,
                             country_name text, 
                             subdivision_code text, 
                             subdivision_name text,
                             city_name text,
                             postal_code text, 
                             continent_code text, 
                             continent_name text,
                             latitude text, 
                             longitude text,
                             time_zone text, 
                             accuracy_radius int, 
                             event_id text
                            );"""

    c = conn.cursor()
    c.execute(create_table)
    conn.commit()
