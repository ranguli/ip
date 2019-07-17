# ip: (the) i(nternet is) p(robably down)
_"ip"_ is a complete stack for the procurement, processing, analysis and visualization of honeypot data.

![](screen4.png)

_Geographical visualization of attacker data_

![](screenshot.png)

_Sample of the SQLite database contents_

![](screenshot3.png)

_Bubble map of attackers based on continent_

## Features:
- Ingests Cowrie honeypot JSON logs into SQLite at `100,000+
  insertions/sec`, while adding geolocation data from MaxMind.
  - Gets the following information on honeypot attackers:
    - Continent, Country, ISP, Region, City, Timezone, and Postal Code 
    - Latitude and Longitude with Accuracy Radius
    - Activity log (login success/fail, logout, credentials used)
    - Log of all access timestamps, as well as timestamp for first and last attacker sightings
    - Number of attacks from an IP on the honeypot
  - Visualizes data out of the box in the following manners:
    - Map IP addresses by geolocation, with color coding and labelling based on severity of threat
    - Chart IP addresses by number of attacks conducted
- Exposes all SQLite data as a Pandas/GeoPandas dataframe, which can be directly manipulated and visualized in the included Jupyter Notebook
- Low memory consumption 

## Structural Overview
![](structural_overview.png)


## Requirements
- System packages:
  `libgeos-dev`, `libgdal-dev`, `libproj-dev`
- For Python requirements see `requirements.txt`
- GeoLite2 City and ASN MMDB files in the root directory of the project, freely downloadable [here](https://dev.maxmind.com/geoip/geoip2/geolite2/)

## Data Size:
One days worth of Cowrie JSON logs are 60MB on average. This means that if the honeypot is running 24/7, 
you'll end up collecting about 20-30GB of _uncompressed_ raw log data _per_ honeypot a year. This is substantially less if you compress the data into tar achives. The SQLite database turns a 60-80MB daily log into roughly 1MB of
processed data. So uncompressed it will yield roughly 365MB a year.


Extrapolating this out to a honeynet containing _5 sensors operated over 3 years_:

(Uncompressed)
- Daily log yield: `~300MB`
- Yearly log yield: `~100GB`
- Total log yield: `~300GB`
- Total SQlite yield: `~1GB`

(Compressed)
- Daily log yield: `~30MB`
- Yearly log yield: `~11GB`
- Total log yield: `~33GB`

## Usage
- Create a virtualenv with `requirements.txt` packages installed
- Run `python log_digester.py`, which will use the sample data provided in the repo
- Run `jupyter notebook` to view the data visualizations

## To-Do:
- Dockerize
- Write a Prometheus exporter
  - [Article](https://medium.com/@ikod/custom-exporter-with-prometheus-b1c23cb24e7a)
  - [Example](https://github.com/MUNComputerScienceSociety/Automata/blob/master/plugins/Analytics/__init__.py)
  - [Article 2](https://www.robustperception.io/productive-prometheus-python-parsing)
  - [Article 3](https://www.robustperception.io/writing-a-jenkins-exporter-in-python)
- Extract and analyze data based on timeframes
  - Get the number of attacks/day, attacks/month, etc 
    - How do we determine the number of attacks for a given day?
      - Need to normalize the timestamps first
  - Get the average frequency of attacks for a timeframe
    (every minute, twice a day, etc)
