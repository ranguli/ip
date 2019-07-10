#!/usr/bin/env python

from netaddr import IPNetwork, IPAddress
from tqdm import tqdm
import sqlite3
import maxminddb

import argparse
import common
import json
import os

def process_log(conn, log_dir, jsonlog):
    c = conn.cursor()

    geolite_city = maxminddb.open_database(common.GEOLITE2_CITY)
    geolite_asn = maxminddb.open_database(common.GEOLITE2_ASN)

    count = 0    
    entries = []

    with open(log_dir + jsonlog) as workload:
        c.execute("BEGIN TRANSACTION")
        for line in tqdm(workload):
            count += 1
            record = json.loads(line)
            
            event_id = record.get("eventid")
            if (event_id == 'cowrie.login.success' or event_id ==
                    'cowrie.login.failed' or event_id == 'cowrie.command.input'):
                if event_id == "cowrie.login.success":
                    attributes = []
                    username = record.get("username")
                    password = record.get("password")
                    attributes.append([username,password])
                elif event_id == "cowrie.command.input":
                    attributes = []
                    attributes.append(record.get("message"))
                else:
                    attributes = None 
            
                src_ip = record.get("src_ip")
                timestamp = record.get("timestamp")
       
                entry = [
                    src_ip,
                    timestamp,
                    event_id,
                    str(attributes)
                ]

                entries.append(entry)
                c.execute("INSERT INTO attack_log(src_ip,timestamp,event_id, attributes) VALUES(?,?,?,?)", entry)
        conn.commit()



def profile_attackers(conn):
    print("Profiling attackers ... this will take a few minutes.")

    geolite_city = maxminddb.open_database(common.GEOLITE2_CITY)
    geolite_asn = maxminddb.open_database(common.GEOLITE2_ASN)

    c = conn.cursor()
    query = c.execute("SELECT * FROM attack_log")

    seen = []
    new_entries = []
    for row in tqdm(query): 
        c = conn.cursor()
        src_ip = row[0]
       
        if src_ip not in seen:
            attack_count = c.execute(
                """SELECT count(*) FROM attack_log WHERE src_ip=? AND 
                (event_id='cowrie.login.success' OR
                event_id='cowrie.login.failed')""", (src_ip,)
            ).fetchone()[0]

            first_seen = c.execute(
                """SELECT * FROM attack_log WHERE src_ip=? LIMIT 1;""",(src_ip,)
            ).fetchone()[1]

            last_seen = c.execute(
                    """ SELECT * FROM attack_log WHERE src_ip=? ORDER BY timestamp 
                    DESC LIMIT 1;""",(src_ip,)
            ).fetchone()[1]

            geolocation = geolite_city.get(src_ip)

            asn_location = geolite_asn.get(src_ip)
            asn = asn_location.get("autonomous_system_organization")

            try:
                country_code = geolocation.get("country").get("iso_code")
            except AttributeError:
                continue

            try:
                country_name = geolocation.get("country").get("names").get("en")
            except AttributeError:
                continue

            try:
                subdivision_name = geolocation.get("subdivisions")
                if subdivision_name is not None:
                    subdivision_name = (
                        subdivision_name[0].get("names").get("en")
                    )
            except AttributeError:
                continue

            try:
                subdivision_code = geolocation.get("subdivisions")
                if subdivision_code is not None:
                    subdivision_code = subdivision_code[0].get("iso_code")
            except AttributeError:
                continue

            try:
                city_name = geolocation.get("city").get("names").get("en")
            except AttributeError:
                continue

            try:
                postal_code = geolocation.get("postal").get("code")
            except AttributeError:
                continue

            try:
                continent_name = geolocation.get("continent").get("names").get("en")
            except AttributeError:
                continue

            try:
                continent_code = (
                    geolocation.get("continent").get("code")
                )
            except AttributeError:
                continue

            try:
                latitude = geolocation.get("location").get("latitude")
            except AttributeError:
                continue

            try:
                longitude = geolocation.get("location").get("longitude")
            except AttributeError:
                continue

            try:
                time_zone = geolocation.get("location").get("time_zone")
            except AttributeError:
                continue

            try:
                accuracy_radius = geolocation.get("location").get(
                    "accuracy_radius"
                )
            except AttributeError:
                continue

            try:
                asn_location = geolite_asn.get(src_ip)
            except AttributeError:
                continue

            new_entry = [
                src_ip,
                asn,
                first_seen,
                last_seen,
                attack_count,
                country_code,
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
                accuracy_radius,
            ]
            new_entries.append(new_entry)

            insertion_statement = """INSERT INTO attacker_profiles(
                                     src_ip,asn,first_seen,last_seen,attack_count,
                                     country_code, country_name, 
                                     subdivision_code, subdivision_name, 
                                     city_name, postal_code, continent_code,
                                     continent_name, latitude, longitude,
                                     time_zone, accuracy_radius) 
                                     VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""

            seen.append(src_ip)
            c.execute(insertion_statement, new_entry)
    conn.commit()

def country_stats(conn):
    c = conn.cursor()

    table = "CREATE TABLE IF NOT EXISTS country_stats(country_name text, attack_count int)"
    c.execute(table)
    countries_query = c.execute("SELECT country_name FROM attacker_profiles").fetchall()
    countries = []
    for country_query in countries_query:
        if country_query[0] not in countries:
            countries.append(country_query[0])

    entries = [] 
    seen = []

    for country in countries:
        query = "SELECT * FROM attacker_profiles WHERE country_name=\'" + country + "\'"
        country_results = c.execute(query).fetchall()

        attack_count_sum = []
        for row in country_results:
            attack_count_sum.append(row[4])
        
        attack_count = sum(attack_count_sum) 
        c.execute("INSERT INTO country_stats(country_name, attack_count) VALUES(?,?)", [country, attack_count])
        conn.commit()

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="ip")
    parser.add_argument(
        "--logdir",
        metavar="D",
        type=str,
        nargs="+",
        help="The number of CPU cores for multicore workload",
    )
    parser.add_argument(
        "--cores",
        metavar="C",
        type=int,
        default=1,
        help="The number of CPU cores for multicore workload",
    )
    args = parser.parse_args()

    if args.cores is not None:
        common.CPU_CORES = args.cores
    if args.logdir is not None:
        common.COWIRE_LOG_DIR = args.log_dir

    print("Connecting to database ....")
    conn = common.connect_db(common.DB_FILE)
    print("Connected ...")

    print("Initializing tables ...")
    #common.init_db(conn)

    #logs = os.listdir(common.COWRIE_LOG_DIR)
    #for index, log in enumerate(logs):
    #    index += 1
    #    print(
    #        "(" + str(index) + "/" + str(len(logs)) + ") Processing logfile " + log + " ..."
    #    )
    #    process_log(conn, common.COWRIE_LOG_DIR, log)
    
    #profile_attackers(conn)
    country_stats(conn)
        
    conn.close()
