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

def import_geolite2(conn):
    """
    The GeoLite2 database is a free geolocation database available here:
    https://dev.maxmind.com/geoip/geoip2/geolite2/

    It has a .csv containing IP blocks, mapped to City IDs.
    An entry may look like: 223.255.192.0/19,1835841,1835841,,0,0

    The corresponding names of those country IDs are in a separate .csv file.
    It may look like: 1835841,en,AS,Asia,KR,"South Korea",0

    The GeoLite2 Country and City databases are updated on the first Tuesday of
    each month. The GeoLite2 ASN database is updated every Tuesday.
    """

    common.init_db(conn)
    c = conn.cursor()

    # Create a lookup table for GeoIDs.
    print("Creating GeoID lookup table ...")
    geo_id_data = {}
    with open(common.CITY_LOCATIONS_CSV) as city_locations_file:
        next(city_locations_file)

        for line in city_locations_file:
            line = line.split(",")

            geo_id = line[0]
            continent_code, continent_name, country_code = line[2:5]
            country_name, region_code, region_name = line[5:8]
            city_name = line[-4].strip("\" ")
            time_zone = line[-2]

            record = {"continent_code": continent_code,
                      "continent_name": continent_name,
                      "country_code": country_code,
                      "country_name": country_name,
                      "region_code": region_code,
                      "region_name": region_name,
                      "city_name": city_name,
                      "time_zone": time_zone
                     }

            geo_id_data[geo_id] = record

    # Create key value pairs of the IP range and its ASN
    print("Creating key value pairs for ASNs ...")
    asn_data = {}
    with open(common.ASN_BLOCKS_CSV) as asn_file:
        next(asn_file)
        for line in asn_file:
            line = line.replace("\"", "")
            line = line.strip().split(",")
            asn_data[line[0]] = line[2]

    # Marry the IP ranges to their GeoIDs
    print("Combining datasets ...")
    with open(common.CITY_BLOCKS_CSV) as city_blocks_file:
        next(city_blocks_file)

        results = []
        print("Adding records to sqlite session ...")
        i = 0
        for line in tqdm(city_blocks_file):
            line = line.split(",")
            ip_range = line[0]
            geo_id = line[1]
            geo_attributes = geo_id_data.get(geo_id)

            if geo_attributes is not None:
                continent_code = geo_attributes.get('continent_code')
                continent_name = geo_attributes.get('continent_name')
                country_code = geo_attributes.get('country_code')
                country_name = geo_attributes.get('country_name')
                region_code = geo_attributes.get('region_code')
                region_name = geo_attributes.get('region_name')
                city_name = geo_attributes.get('city_name')
                time_zone = geo_attributes.get('time_zone')
             
                postal_code, latitude, longitude, accuracy = line[6:10]
                asn = asn_data.get(ip_range)

                # Write this all out to SQL

                results.append([ip_range, continent_code, continent_name,
                                country_code, country_name, region_code, region_name,
                                city_name, asn, time_zone, postal_code, latitude, longitude,
                                accuracy])

                i += 1
               
                # Commit chunks of 5k records at a time. This increases the speed *significantly*
                # to roughly 100,000 insertions/s. Not bad.
                if i == 5000:
                    c.execute("BEGIN TRANSACTION")
                    for result in results:
                        insertion_statement = """ INSERT INTO geolocation(
                                                  ip_range,continent_code,continent_name,country_code,country_name,region_code,
                                                  region_name,city_name,asn,time_zone,postal_code,latitude,longitude,accuracy) 
                                                  VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) """ 
                        c.execute(insertion_statement, result)
                    results = []
                    i = 0

def process_log(conn, jsonlog):
    print("Processing logfile")
    c = conn.cursor()
    
    count = 0
    results = []
  
    reader = maxminddb.open_database('GeoLite2-City.mmdb')
    with open(jsonlog) as workload:
        c.execute("BEGIN TRANSACTION")

        for line in tqdm(workload):
            record = json.loads(line)
            event_id = record.get('eventid')
            
            if (event_id == "cowrie.login.success") or (event_id == "cowrie.login.failed"):
                session = record.get('session')
                src_ip = record.get('src_ip')
                timestamp = record.get('timestamp')
                geolocation = reader.get(src_ip)
                print(geolocation)
                result = [session, src_ip, timestamp, event_id]
                results.append(result)
                        
                insertion_statement = """INSERT INTO attack_log(
                                         session,src_ip,timestamp,event_id) 
                                         VALUES(?,?,?,?)""" 
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
    
    conn = common.connect_db(common.DB_FILE)
    
    common.init_db(conn)
    #import_geolite2(conn)
    
    #query = load_geolocation()
    process_log(conn, common.JSONLOG) 
    
    # Get all unique session IDs, as a way to identify data across pools 

    conn.close()
