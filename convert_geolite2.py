#!/usr/bin/env python
import csv

"""
The GeoLite2 database is a free geolocation database available here:
https://dev.maxmind.com/geoip/geoip2/geolite2/

It has a .csv containing IP blocks, mapped to country IDs.
An entry may look like: 223.255.192.0/19,1835841,1835841,,0,0

The corresponding names of those country IDs are in a separate .csv file.
It may look like: 1835841,en,AS,Asia,KR,"South Korea",0

This script simple merges the two together into one unified .csv file.
"""

# Create a dictionary of a country's location code, as well as its name
# Ex:
# {"123123": ["NA", "North America", "NI", "Hondarus"]}

country_locations = {}

with open("GeoLite2-Country-Locations-en.csv") as f:
    for line in f:
        line = line.strip().split(",")
        country_locations[line[0]] = [line[2], line[3], line[4], line[5]] 

with open("GeoLite2-Country-Blocks-IPv4.csv") as ip_blocks_file:
    with open("ip_geolocation.csv", "w+") as outfile:
        writer = csv.writer(outfile)
        for line in ip_blocks_file: 
            line = line.split(",")
            
            ip_range = line[0]
            country_id = line[1]

            country_location = country_locations.get(country_id)
            if country_location is not None:
                country_location = [i.replace('"', '') for i in country_location]
                row = [ip_range] + country_location 
                writer.writerow(row)
