#!/usr/bin/env python

from netaddr import IPNetwork, IPAddress
import sqlite3 
from tqdm import tqdm

import maxminddb

from ctypes import * 
import argparse
import common
import json
import os
import re

def process_log(conn, jsonlog):
    print("Processing logfile")
    c = conn.cursor()
    
    count = 0
    results = []
  
    geolite_city = maxminddb.open_database('GeoLite2-City.mmdb')
    geolite_asn = maxminddb.open_database('GeoLite2-ASN.mmdb')
    with open(jsonlog) as workload:
        c.execute("BEGIN TRANSACTION")

        for line in tqdm(workload):
            record = json.loads(line)
            event_id = record.get('eventid')
            
            if (event_id == "cowrie.login.success") or (event_id == "cowrie.login.failed"):
                session = record.get('session')
                src_ip = record.get('src_ip')
                timestamp = record.get('timestamp')

                geolocation = geolite_city.get(src_ip)
                asn_location = geolite_asn.get(src_ip)

                asn = asn_location.get('autonomous_system_organization')

                try:
                    country_code = geolocation.get('country').get('iso_code')
                except AttributeError:
                    continue

                
                try:
                    country_name = geolocation.get('country').get('names').get('en')
                except AttributeError:
                    continue

                try: 
                    subdivision_name = geolocation.get('subdivisions')
                except AttributeError:
                    continue
               
                if subdivision_name is not None:
                    subdivision_name = subdivision_name[0].get('names').get('en')

                subdivision_code = geolocation.get('subdivisions')
                
                if subdivision_code is not None:
                    subdivision_code = subdivision_code[0].get('iso_code')

                try:
                    city_name = geolocation.get('city').get('names').get('en')
                except AttributeError:
                    continue

                try:
                    postal_code = geolocation.get('postal').get('code')
                except AttributeError:
                    continue

                try:
                    continent_name = geolocation.get('continent').get('code')
                except AttributeError:
                    continue
                
                try:
                    continent_code = geolocation.get('continent').get('names').get('en')
                except AttributeError:
                    continue

                try:
                    latitude = geolocation.get('location').get('latitude') 
                except AttributeError:
                    continue

                try:
                    longitude = geolocation.get('location').get('longitude') 
                except AttributeError:
                    continue

                try:
                    timezone = geolocation.get('location').get('timezone') 
                except AttributeError:
                    continue
               
                try:
                    accuracy_radius = geolocation.get('location').get('accuracy') 
                except AttributeError:
                    continue

                try:
                    asn_location = geolite_asn.get(src_ip)
                except AttributeError:
                    continue
                
                result = [session, src_ip, asn,timestamp, country_code,
                        country_name, subdivision_name, subdivision_code,
                        city_name, postal_code, continent_name, continent_code,
                        latitude, longitude, timezone, accuracy_radius,
                        event_id]
                results.append(result)
                        
                insertion_statement = """INSERT INTO attack_log(
                                         session,src_ip,asn,timestamp,country_code,
                                         country_name, subdivision_name,
                                         subdivision_code, city_name,
                                         postal_code, continent_name,
                                         continent_code, latitude, longitude,
                                         timezone, accuracy_radius, event_id) 
                                         VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""" 
                c.execute(insertion_statement, result)
                
                count += 1 
                if count == 5000:
                    conn.commit()
                    count = 0
                    results = []

    conn.commit()
    c.close()
    conn.close()

if __name__ == '__main__':

    #parser = argparse.ArgumentParser(description="yeet")
    
    #parser.add_argument("--init-db", action="store_true", dest="initdb",
    #        help="Create database structure if not already created")
    #parser.add_argument("--log-dir", metavar="D", type=str, nargs="+",
    #        help="The number of CPU cores for multicore workload")
    #parser.add_argument("--import_geolite", action="store_true", help="")
    #parser.add_argument("--cores", metavar="C", type=int, default=1,
    #        help="The number of CPU cores for multicore workload")
    #args = parser.parse_args()

    #if args.cores is not None:
    #    common.CPU_CORES = args.cores
    #common.COWIRE_LOG_DIR = args.log_dir

    #logs = os.listdir(common.COWRIE_LOG_DIR)
    #sessions = common.get_session_ids(logs)
   
    print("Connecting to database ....")
    conn = common.connect_db(common.DB_FILE)
   
    print("Creating tables ...") 
    common.init_db(conn)

    process_log(conn, common.JSONLOG) 
    
    # Get all unique session IDs, as a way to identify data across pools 

    conn.close()
