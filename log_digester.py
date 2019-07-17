#!/usr/bin/env python

from netaddr import IPNetwork, IPAddress
from tqdm import tqdm
import maxminddb
import iso3166

import sqlite3
import argparse
import json
import os
import base64

SCHEMA_FILE = "create_schema.sql"
VIEWS_FILE = "create_views.sql"

JSONLOG = "cowrie.json.2019-07-04"
CITY_LOCATIONS_CSV = "GeoLite2-City-Locations-en.csv"
GEOLITE2_CITY = "GeoLite2-City.mmdb"
GEOLITE2_ASN = "GeoLite2-ASN.mmdb"

COWRIE_LOG_DIR = "cowrie/"
DB_FILE = "db.sqlite"

geolocation_db = maxminddb.open_database(GEOLITE2_CITY)

def connect_db(db_file):
    """ Connects to SQLite database """

    connection = None
    try:
        connection = sqlite3.connect(db_file)
    except (Exception, sqlite3.Error) as error:
        print(error)
    return connection

def create_schema(conn, schema_file):
    c = conn.cursor()
    with open(schema_file) as f:
        commands = f.read().split(";")
        for command in commands:
            c.execute(command)

def create_views(conn, VIEWS_FILE):
    c = conn.cursor()
    with open(VIEWS_FILE) as f:
        commands = f.read().split(";")
        for command in commands:
            c.execute(command)
    conn.commit()

def geolocate(src_ip):
    
    geolocation_results  = []
    geolocation_query = [
        ["country", "iso_code"],
        ["country", "names", "en"],
        ["subdivisions", 0, "names", "en"],
        ["subdivisions", 0, "iso_code"],
        ["city", "names", "en"],
        ["postal", "code"],
        ["continent", "names", "en"],
        ["continent", "code"],
        ["location", "latitude"],
        ["location", "longitude"],
        ["location", "time_zone"],
        ["location", "accuracy_radius"],
    ]

    for geolocation_attribute in geolocation_query:
        try:
            result = geolocation_db.get(src_ip).get(geolocation_attribute.pop(0))
            for key in geolocation_attribute:
                if isinstance(key, int):
                    result = result[key]
                else:
                    result = result.get(key)
        except(AttributeError, TypeError) as e:
            result = None
        geolocation_results.append(result)
    
    # Get the ISO 3 digit country code from the 2 digit code. This is needed
    # for the GeoJSON choropleth map in the Python notebook.

    try:
        country_code_alpha3 = iso3166.countries.get(geolocation_results[0]).alpha3 
    except KeyError:
        country_code_alpha3 = None

    geolocation_results.insert(1, country_code_alpha3)
    return geolocation_results

def process_log(conn, log_directory, jsonlog):
    c = conn.cursor()

    # The MaxMind Database files for performing geolocation on an IP address,
    # as well as the geolocation of an ASN (an ID number for an ISP).
    isp_geolocation_db = maxminddb.open_database(GEOLITE2_ASN)
    
    entries = []
    # Cache IPs after we geolocate them to greatly increase processing speed
    geolocated_ip_addresses = {} 

    with open(log_directory + jsonlog) as logfile:
        c.execute("BEGIN TRANSACTION")
        for line in tqdm(logfile):
            log_entry = json.loads(line)

            # Event IDs are identifiers that Cowrie uses to signify what 
            # type of action an attacker is doing on a honeypot.
            event_id = log_entry.get("eventid")
            
            if (
                event_id == "cowrie.login.success"
                or event_id == "cowrie.login.failed"
                or event_id == "cowrie.command.input"
            ):
                if event_id == "cowrie.command.input":
                    attempted_username = "" 
                    attempted_password = "" 
                    command = log_entry.get("message")
                else: 
                    attempted_username = log_entry.get("username")
                    attempted_password = log_entry.get("password")
                    command = None 

                src_ip = log_entry.get("src_ip")
                event_timestamp = log_entry.get("timestamp")
                credential_signature = base64.b64encode(
                        attempted_username.encode() + attempted_password.encode()
                )
                
                isp_name = isp_geolocation_db.get(src_ip).get("autonomous_system_organization")

                entry = [
                    src_ip, 
                    isp_name, 
                    event_timestamp, 
                    event_id, 
                    attempted_username, 
                    attempted_password,
                    credential_signature,
                    command
                ]

                if src_ip not in geolocated_ip_addresses.keys():
                    geolocation_results = geolocate(src_ip)
                    geolocated_ip_addresses[src_ip] = geolocation_results

                elif src_ip in geolocated_ip_addresses.keys():
                    geolocation_results = geolocated_ip_addresses[src_ip] 

                entry.extend(geolocation_results)
                insertion_statement = """ INSERT INTO attack_log(
                                             src_ip,
                                             isp_name,
                                             event_timestamp,
                                             event_id,
                                             attempted_username,
                                             attempted_password,
                                             credential_signature,
                                             command,
                                             country_code, 
                                             country_code_alpha3,
                                             country_name, 
                                             subdivision_code, 
                                             subdivision_name, 
                                             city_name, 
                                             postal_code, 
                                             continent_code,
                                             continent_name, 
                                             latitude, 
                                             longitude,
                                             time_zone, 
                                             accuracy_radius)
                                         VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                                    """

                entries.append(entry)
        for entry in entries:
            c.execute(insertion_statement, entry)
        c.execute("END TRANSACTION")
        conn.commit()

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

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="ip")
    parser.add_argument(
        "--log-dir",
        dest="log_directory",
        type=str,
        nargs="+",
        help="The log directory for cowrie data",
    )
    parser.add_argument(
        "--no-processing", dest="no_processing", default=False, action="store_true"
    )
    args = parser.parse_args()

    if args.log_directory is None:
        print("Assuming default log directory: " + COWRIE_LOG_DIR)
    else:
        COWIRE_LOG_DIR = args.log_directory
        print("Using log directory: " + "".join(args.log_directory))

    print("Connecting to database ....")
    conn = connect_db(DB_FILE)
    print("Connected ...")

    print("Creating schema ...")
    create_schema(conn, SCHEMA_FILE)

    if not args.no_processing:
        logs = os.listdir(COWRIE_LOG_DIR)
        for index, log in enumerate(logs):
            index += 1
            print(
                "("
                + str(index)
                + "/"
                + str(len(logs))
                + ") Processing logfile "
                + log
                + " ..."
            )
            process_log(conn, COWRIE_LOG_DIR, log)
    print("Creating views ...")
    create_views(conn, VIEWS_FILE)

