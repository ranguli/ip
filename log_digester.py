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

    count = 0
    updated_entries = []
    new_entries = []

    geolite_city = maxminddb.open_database(common.GEOLITE2_CITY)
    geolite_asn = maxminddb.open_database(common.GEOLITE2_ASN)

    with open(log_dir + jsonlog) as workload:
        seen = []
        c.execute("BEGIN TRANSACTION")

        for line in tqdm(workload):
            record = json.loads(line)
            event_id = record.get("eventid")

            if (event_id == "cowrie.login.success") or (
                event_id == "cowrie.login.failed"
            ):

                src_ip = record.get("src_ip")
                exists = c.execute(
                    "SELECT count(*) FROM attack_log WHERE src_ip=?", (src_ip,)
                ).fetchall()[0][0]

                # If the IP already exists, just update its timestamp attributes
                if (exists > 0) and (src_ip not in seen):
                    attack_count = c.execute(
                        "SELECT attack_count FROM attack_log WHERE src_ip=?", (src_ip,)
                    )
                    c.execute(
                        "UPDATE attack_log SET attack_count = attack_count + 1 WHERE src_ip=?",
                        (src_ip,),
                    )

                    timestamp = record.get("timestamp")
                    # print(timestamp)
                    # Get all existing timestamps in the DB for a particular IP
                    timestamps = c.execute(
                        "SELECT timestamps FROM attack_log \
                            WHERE src_ip=?",
                        (src_ip,),
                    ).fetchall()

                    timestamps = [("".join(timestamps[0]))]
                    timestamps.append(timestamp + ",")

                    first_seen = timestamps[0].split(",")[0]
                    last_seen = timestamps[-1]
                    attack_count = len(timestamps[0].split(","))

                    # Format our results back into a string so we can insert it into
                    # sqlite

                    update_statement = """ UPDATE attack_log SET 
                                            timestamps=?,
                                            first_seen=?,
                                            last_seen=?,
                                            attack_count=?
                                            WHERE src_ip=?
                                   """

                    c.execute(
                        update_statement,
                        [
                            "".join(timestamps),
                            first_seen,
                            last_seen,
                            attack_count,
                            src_ip,
                        ],
                    )
                    timestamps = None

                elif exists == 0:
                    attack_count = 1
                    seen.append(src_ip)
                    timestamp = record.get("timestamp") + ","

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
                        timestamp,
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
                        event_id,
                    ]
                    new_entries.append(new_entry)

                    insertion_statement = """INSERT INTO attack_log(
                                             src_ip,asn,timestamps,attack_count,country_code,
                                             country_name, subdivision_code,
                                             subdivision_name, city_name,
                                             postal_code, continent_code,
                                             continent_name, latitude, longitude,
                                             time_zone, accuracy_radius, event_id) 
                                             VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""

                    c.execute(insertion_statement, new_entry)
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
    common.init_db(conn)

    logs = os.listdir(common.COWRIE_LOG_DIR)
    for index, log in enumerate(logs):
        index += 1
        print(
            "(" + str(index) + "/" + str(len(logs)) + ") Processing logfile " + log + " ..."
        )
        process_log(conn, common.COWRIE_LOG_DIR, log)

    conn.close()
