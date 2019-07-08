import sqlite3 
import os

from netaddr import IPAddress, IPNetwork

JSONLOG = "cowrie.json.2019-07-04"
CITY_LOCATIONS_CSV = "GeoLite2-City-Locations-en.csv"
CITY_BLOCKS_CSV = "GeoLite2-City-Blocks-IPv4.csv"
ASN_BLOCKS_CSV = "GeoLite2-ASN-Blocks-IPv4.csv"

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

def geolocate(conn, ip):
    """ Queries the GeoLite2 table in SQLite for information on
        an IP address.
    """

    cache = {} 

    c = conn.cursor()

    first_octet = ip.split(".")[0]
    # Select all IP ranges that start with the first octet of our IP
    query = c.execute("SELECT * FROM geolocation WHERE ip_range LIKE '" + first_octet +"%'")
   
    if cache.get(ip):
        return cache.get(ip)
   
    for result in query:
        if IPAddress(ip) in IPNetwork(result[0]):
            if result not in cache:
                cache[ip] = result
            return(result)
            break

def connect_db(db_file):
    """ Connects to SQLite database """

    connection = None
    try:
        connection = sqlite3.connect(db_file)
    except (Exception, sqlite3.Error) as error:
        print(error)
    return connection 

def init_db(conn): 
    """ Creates all the necessary database tables """

    tables = []

    geolocation_table = """ CREATE TABLE IF NOT EXISTS geolocation (
                                    ip_range text NOT NULL,
                                    continent_code text,
                                    continent_name text,
                                    country_code text,
                                    country_name text,
                                    region_code text,
                                    region_name text,
                                    city_name text,
                                    asn text,
                                    time_zone text,
                                    postal_code text,
                                    latitude text,
                                    longitude text,
                                    accuracy text
                                ); """
   
    attack_table = """ CREATE TABLE IF NOT EXISTS attack_log (
                                session text,
                                src_ip text,
                                timestamp text,
                                event_id text 
                              );"""
    
    tables.append(geolocation_table) 
    tables.append(attack_table) 

    c = conn.cursor()

    for table in tables:
        c.execute(table)

    conn.commit()

