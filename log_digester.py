#!/usr/bin/env python

from netaddr import IPNetwork, IPAddress
from tqdm import tqdm
import maxminddb
import iso3166
import csv

import sqlite3
import argparse
import os

SCHEMA_FILE = "create_schema.sql"
VIEWS_FILE = "create_views.sql"

CSVLOG = "AllTraffic_1.sample.csv"
CITY_LOCATIONS_CSV = "GeoLite2-City-Locations-en.csv"
GEOLITE2_CITY = "GeoLite2-City.mmdb"
GEOLITE2_ASN = "GeoLite2-ASN.mmdb"

LOG_DIR = "./cowrie/"
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

def geolocate(source_ip):
    
    geolocation_results  = []
    geolocation_query = [
        ["country", "iso_code"],
        ["country", "names", "en"],
        ["subdivisions", 0, "names", "en"],
        ["subdivisions", 0, "iso_code"],
        ["city", "names", "en"],
        ["location", "latitude"],
        ["location", "longitude"]
    ]

    for geolocation_attribute in geolocation_query:
        try:
            result = geolocation_db.get(source_ip).get(geolocation_attribute.pop(0))
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

def process_log(conn, log_directory, log):
    c = conn.cursor()

    # The MaxMind Database files for performing geolocation on an IP address,
    # as well as the geolocation of an ASN (an ID number for an ISP).
    isp_geolocation_db = maxminddb.open_database(GEOLITE2_ASN)
    
    entries = []
    # Cache IPs after we geolocate them to greatly increase processing speed
    geolocated_ip_addresses = {} 

    with open(log_directory + log) as logfile:
        c.execute("BEGIN TRANSACTION")
        reader = csv.DictReader(logfile, delimiter="\t")
        for row in tqdm(reader):
            
            source_ip = row.get("source_ip")
            entry = [source_ip]

            if source_ip not in geolocated_ip_addresses.keys():
                geolocation_results = geolocate(source_ip)
                geolocated_ip_addresses[source_ip] = geolocation_results

            elif source_ip in geolocated_ip_addresses.keys():
                geolocation_results = geolocated_ip_addresses[source_ip] 

            entry.extend(geolocation_results)
            insertion_statement = """ INSERT INTO attack_log(
                                         source_ip,
                                         country_code, 
                                         country_code_alpha3,
                                         country_name, 
                                         subdivision_code, 
                                         subdivision_name, 
                                         city_name, 
                                         latitude, 
                                         longitude
                                         )
                                     VALUES(?,?,?,?,?,?,?,?,?)
                                """

            entries.append(entry)
        for entry in entries:
            c.execute(insertion_statement, entry)
        c.execute("END TRANSACTION")
        conn.commit()

def chunk(log, chunk_count):
    """ Divides a file into equal sized chunks. 
        Useful for dividing logs up for multiprocessing 
    """
    chunks = []

    length = log_length(log)
    chunk_size = length // chunk_count

    with open(log) as f:
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
    conn = connect_db(DB_FILE)
    create_views(conn, VIEWS_FILE)
    exit()
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
        print("Assuming default log directory: " + LOG_DIR)
    else:
        COWIRE_LOG_DIR = args.log_directory
        print("Using log directory: " + "".join(args.log_directory))

    print("Connecting to database ....")
    conn = connect_db(DB_FILE)
    print("Connected ...")

    print("Creating schema ...")
    create_schema(conn, SCHEMA_FILE)

    if not args.no_processing:
        logs = os.listdir(LOG_DIR)
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
            process_log(conn, LOG_DIR, log)
    print("Creating views ...")
    create_views(conn, VIEWS_FILE)

