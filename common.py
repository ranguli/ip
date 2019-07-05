import sqlite3
import os

from netaddr import IPAddress, IPNetwork

def geolocate(ip, c):
    c.execute('SELECT * FROM geolocation') 
    for row in c:
        if IPAddress(ip) in IPNetwork(row[0]):
            print(row) 
            break

def connect_db(db_file):
    if not os.path.exists(db_file):
        with open(db_file, 'w'): 
            pass
    conn = sqlite3.connect(db_file)
    conn.text_factory = str
    if conn is not None:
        return(conn.cursor())

def create_geolocation_table(c): 
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
    c.execute(geolocation_table_sql)

