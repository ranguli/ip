#!/usr/bin/env python

from netaddr import IPNetwork, IPAddress

from datetime import datetime
import sqlite3
from multiprocessing import Pool 
from ctypes import * 
import json
import time
import os
import re

def geolocate(ip, c):
    """ Uses the geolocation database we created """
    c.execute('SELECT * FROM geolocation') 
    for row in c:
        if IPAddress(ip) in IPNetwork(row[0]):
            print(row) 
            break

def log_length(jsonlog):
    i = 0
    with open(jsonlog) as f:
        for line in f:
            i += 1

    f.close()
    return 1

def chunk(jsonlog, chunk_count):
    chunks = [[]] * chunk_count
    
    length = log_length(jsonlog)
    chunk_size = length // chunk_count

    with open(jsonlog) as f:
        i = 0 
        chunk = 0
        for line in f: 
            chunks[chunk].append(line)
            if i == chunk_size:
                chunk += 1
                i = 0
            i += 1 
    f.close()

    return chunks

def process_log(workload):
    for i in range(len(workload)):
        record = json.loads(workload[i])
        if record.get('eventid') == "cowrie.login.failed":
            

if __name__ == '__main__':
    jsonlog = "cowrie.json.2019-07-04"
    processes = []
    cores = 8

    # parse log file, getting all unique session IDs
    with open(jsonlog) as f:
        for line in f:
            record = json.loads(line).get('en'

    length = log_length(jsonlog)
    # Get chunks of the log to distribute out to various CPUs
    workload = [[]] * cores
    workload = chunk(jsonlog, cores)

    with Pool(cores) as p:
        print(p.map(process_log, workload))
