#!/usr/bin/env python

"""
The GeoLite2 database is a free geolocation database available here:
https://dev.maxmind.com/geoip/geoip2/geolite2/

It has a .csv containing IP blocks, mapped to City IDs.
An entry may look like: 223.255.192.0/19,1835841,1835841,,0,0

The corresponding names of those country IDs are in a separate .csv file.
It may look like: 1835841,en,AS,Asia,KR,"South Korea",0

This script simple merges the two together into one unified .csv file.
"""

import os
import csv
import sqlite3

from tqdm import tqdm 

city_locations_csv = "GeoLite2-City-Locations-en.csv"
city_blocks_csv = "GeoLite2-City-Blocks-IPv4.csv" 
asn_blocks_csv = "GeoLite2-ASN-Blocks-IPv4.csv"
db_file = "db.sqlite"


# Boilerplate SQL query to create our table 
geolocation_table_sql = """ CREATE TABLE IF NOT EXISTS geolocation (
                                ip_range text NOT NULL,
                                continent_code text,
                                continent_name text,
                                country_code text,
                                country_name text,
                                region_code text,
                                region_name text,
                                city_name,
                                asn text,
                                time_zone text,
                                postal_code text,
                                latitude text,
                                longitude text,
                                accuracy text
                            ); """

try:
    if not os.path.exists(db_file):
        with open(db_file, 'w'): pass
    conn = sqlite3.connect(db_file)
    conn.text_factory = str
except Error as e:
    print(e)

if conn is not None:
    c = conn.cursor()
    c.execute(geolocation_table_sql)

# Create a lookup table for GeoIDs. 
print("Creating GeoID lookup table ...")
geo_id_data = {} 
with open(city_locations_csv) as city_locations_file:
    next(city_locations_file)

    for line in city_locations_file:
        line  = line.split(",")

        geo_id = line[0]
        continent_code, continent_name, country_code = line[2:5]
        country_name, region_code, region_name = line[5:8]
        city_name = line[-4].strip("\" ")
        time_zone = line[-2]

        record = {  "continent_code": continent_code, 
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
with open(asn_blocks_csv) as asn_file:
    next(asn_file)
    for line in asn_file:
        line = line.replace("\"", "")
        line = line.strip().split(",")
        asn_data[line[0]] = line[2]

# Marry the IP ranges to their GeoIDs    
print("Combining datasets ...")
with open(city_blocks_csv) as city_blocks_file:
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
            country_code   = geo_attributes.get('country_code')
            country_name   = geo_attributes.get('country_name')
            region_code    = geo_attributes.get('region_code')
            region_name    = geo_attributes.get('region_name')
            city_name      = geo_attributes.get('city_name')
            time_zone      = geo_attributes.get('time_zone')
         
            postal_code, latitude, longitude, accuracy = line[6:10]
            asn = asn_data.get(ip_range)

            # Write this all out to SQL
            #geo_data = [i.replace('"', '') for i in geo_data]

            results.append([ip_range, continent_code, continent_name,
                country_code, country_name, region_code, region_name,
                city_name, asn, time_zone, postal_code, latitude, longitude,
                accuracy])

            i += 1
           
            # Commit chunks of 5k records at a time. This increases the speed *significantly*
            # to roughly 500,000 insertions/m. Not bad.
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

