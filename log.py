#!/usr/bin/env python
import os

from dotenv import load_dotenv
load_dotenv()
connections = []


API_KEY = os.getenv("API_KEY")

with open("auth.log","r") as f:
    for line in f:
        if "Disconnected" in line:
            line = line.split()
            line = line[9:len(line)-1]
            print(line)
            connections.append(line)