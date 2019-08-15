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

CSVLOG = "cowrie_1.csv"

LOG_DIR = "./cowrie/"
DB_FILE = "db.sqlite"

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

def process_log(conn, log_directory, log):
    c = conn.cursor()
    entries = []

    with open(log_directory + log) as logfile:
        c.execute("BEGIN TRANSACTION")
        reader = csv.DictReader(logfile, delimiter="\t")
        for row in tqdm(reader):
            event_timestamp = row.get("timestamp").split(" ")[0]
            entry = [event_timestamp]
            insertion_statement = """ INSERT INTO attack_log(
                                         event_timestamp
                                         )
                                     VALUES(?)
                                """

            entries.append(entry)
        for entry in entries:
            c.execute(insertion_statement, entry)
        c.execute("END TRANSACTION")
        conn.commit()

if __name__ == "__main__":
    print("Connecting to database ....")
    conn = connect_db(DB_FILE)
    print("Connected ...")

    print("Creating schema ...")
    create_schema(conn, SCHEMA_FILE)

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

    print("Creating views...")
    create_views(conn, VIEWS_FILE)

