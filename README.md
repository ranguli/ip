# ip
![screenshot](screenshot.png)
![screenshot2](screenshot2.png)

## Features:
- Parses Cowrie honeypot `json` logs into `sqlite` at roughly `20,000
  insertions/sec`, adding geolocation data from MaxMind.
- Low memory consumption 

## Requirements
- `libgeos-dev`, `libgdal-dev`, `libproj-dev`
- `basemap`

## To-Do:
- Extract and analyze data based on timeframes
  - Get the number of attacks/day, attacks/month, etc 
  - Get the average frequency of attacks for a timeframe
    (every minute, twice a day, etc)
