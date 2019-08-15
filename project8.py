#!/usr/bin/env python

from netaddr import IPNetwork, IPAddress
from tqdm import tqdm

import csv
import ast
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
    
    entries = []
    insertion_statement = """ INSERT INTO attack_log(
                                 username,
                                 password
                                 )
                             VALUES(?,?)
                        """
    with open(log_directory + log) as logfile:
        c = conn.cursor()
        c.execute("BEGIN TRANSACTION")
        reader = csv.DictReader(logfile, delimiter="\t")
        for row in tqdm(reader):
            credentials = ast.literal_eval(row.get("credentials"))
            for element in credentials:
                username, password = element
                entry = username, password
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

