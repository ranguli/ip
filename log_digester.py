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
            if (
                event_id == "cowrie.login.success"
                or event_id == "cowrie.login.failed"
                or event_id == "cowrie.command.input"
            ):
                if event_id == "cowrie.login.success":
                    attributes = []
                    username = record.get("username")
                    password = record.get("password")
                    attributes.append([username, password])
                elif event_id == "cowrie.command.input":
                    attributes = []
                    attributes.append(record.get("message"))
                else:
                    attributes = None

                src_ip = record.get("src_ip")
                timestamp = record.get("timestamp")

                entry = [src_ip, timestamp, event_id, str(attributes)]

                entries.append(entry)
                c.execute(
                    "INSERT INTO attack_log(src_ip,timestamp,event_id, attributes) VALUES(?,?,?,?)",
                    entry,
                )
        conn.commit()


def profile_attackers(conn):
    print("Profiling attackers ... this will take a few minutes.")

    geolite_city = maxminddb.open_database(common.GEOLITE2_CITY)
    geolite_asn = maxminddb.open_database(common.GEOLITE2_ASN)

    c = conn.cursor()
    query = c.execute("SELECT * FROM attack_log").fetchall()
    unique_ip_addrs = list(dict.fromkeys([i[0] for i in query]))

    entries = []
    for src_ip in tqdm(unique_ip_addrs):
        attack_count = 0
        first_seen = ""
        
        hits = []
        for i , row in enumerate(query):
            if row[0] == src_ip:
                if row[2] == "cowrie.login.success" or row[2] == "cowrie.login.failed":
                    attack_count += 1
                hits.append(i)
        
        first_seen = query[hits[0]][1]
        last_seen = query[hits[-1]][1]
        
        geolocation = geolite_city.get(src_ip)

        asn_location = geolite_asn.get(src_ip)
        asn = asn_location.get("autonomous_system_organization")

        try:
            country_code = geolocation.get("country").get("iso_code")
        except AttributeError:
            pass

        try:
            country_name = geolocation.get("country").get("names").get("en")
        except AttributeError:
            pass

        try:
            subdivision_name = geolocation.get("subdivisions")
            if subdivision_name is not None:
                subdivision_name = subdivision_name[0].get("names").get("en")
        except AttributeError:
            pass

        try:
            subdivision_code = geolocation.get("subdivisions")
            if subdivision_code is not None:
                subdivision_code = subdivision_code[0].get("iso_code")
        except AttributeError:
            pass

        try:
            city_name = geolocation.get("city").get("names").get("en")
        except AttributeError:
            pass

        try:
            postal_code = geolocation.get("postal").get("code")
        except AttributeError:
            pass

        try:
            continent_name = geolocation.get("continent").get("names").get("en")
        except AttributeError:
            pass

        try:
            continent_code = geolocation.get("continent").get("code")
        except AttributeError:
            pass

        try:
            latitude = geolocation.get("location").get("latitude")
        except AttributeError:
            pass

        try:
            longitude = geolocation.get("location").get("longitude")
        except AttributeError:
            pass

        try:
            time_zone = geolocation.get("location").get("time_zone")
        except AttributeError:
            pass

        try:
            accuracy_radius = geolocation.get("location").get("accuracy_radius")
        except AttributeError:
            pass

        try:
            asn_location = geolite_asn.get(src_ip)
        except AttributeError:
            pass

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

        entries.append(new_entry)

    for new_entry in entries:
        insertion_statement = """INSERT INTO attacker_profiles(
                                 src_ip,asn,first_seen,last_seen,attack_count,
                                 country_code, country_name, 
                                 subdivision_code, subdivision_name, 
                                 city_name, postal_code, continent_code,
                                 continent_name, latitude, longitude,
                                 time_zone, accuracy_radius) 
                                 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""

        c.execute(insertion_statement, new_entry)
    conn.commit()

def country_stats(conn):
    c = conn.cursor()

    table = (
        "CREATE TABLE IF NOT EXISTS country_stats(country_name text, attack_count int)"
    )
    c.execute(table)
    countries_query = c.execute("SELECT country_name FROM attacker_profiles").fetchall()
    countries = []
    for country_query in countries_query:
        if country_query[0] not in countries:
            countries.append(country_query[0])

    entries = []
    seen = []

    for country in countries:
        query = "SELECT * FROM attacker_profiles WHERE country_name='" + country + "'"
        country_results = c.execute(query).fetchall()

        attack_count_sum = []
        for row in country_results:
            attack_count_sum.append(row[4])

        attack_count = sum(attack_count_sum)
        c.execute(
            "INSERT INTO country_stats(country_name, attack_count) VALUES(?,?)",
            [country, attack_count],
        )
        conn.commit()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="ip")
    parser.add_argument(
        "--log-dir",
        dest='log_dir',
        type=str,
        nargs="+",
        help="The log directory for cowrie data",
    )
    parser.add_argument(
            '--no-processing', 
            dest='no_processing', 
            default=True,
            action='store_true'
    )
    args = parser.parse_args()

    if args.log_dir is None:
        print("Assuming default log directory: " + common.COWRIE_LOG_DIR)
    else:
        common.COWIRE_LOG_DIR = args.log_dir
        print("Using log directory: " + "".join(args.log_dir))

    print("Connecting to database ....")
    conn = common.connect_db(common.DB_FILE)
    print("Connected ...")

    print("Initializing tables ...")
    common.init_db(conn)


    if not args.no_processing:
        logs = os.listdir(common.COWRIE_LOG_DIR)
        for index, log in enumerate(logs):
           index += 1
           print(
               "(" + str(index) + "/" + str(len(logs)) + ") Processing logfile " + log + " ..."
           )
           process_log(conn, common.COWRIE_LOG_DIR, log)
    elif args.no_processing: 
        print("Not processing logs")
        profile_attackers(conn)
        #country_stats
