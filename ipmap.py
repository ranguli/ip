#!/usr/bin/env python
import os
import json
import csv
import webbrowser

import requests
import pycountry
import folium
import pandas as pd

def main():
    url = 'https://freegeoip.app/json'
    stats = {'Country_Code': 'Connections'}
    for country in pycountry.countries:
        stats[country.alpha_3] = 0

    processed = []
    with open("auth.log","r") as f:
        for line in f:
            if "Disconnected" in line: # Grab the line where the bot disconnects
                ip = line.split()[10]
                if not any(c.isalpha() for c in ip) and ip not in processed: # Check that the IP we grabbed is good
                    response = requests.request("GET", str(url + "/" + ip))
                    if response.status_code == requests.codes.ok:
                        alpha_2_code = response.json().get('country_code')
                        if alpha_2_code == '': # Sometimes there are spooky IP's with no country code
                            print("??" + "\t" + str(response.json().get('ip')))
                            processed.append(ip)
                        else:
                            print(alpha_2_code + "\t" + str(response.json().get('ip')))
                            alpha_3_code = pycountry.countries.get(alpha_2=alpha_2_code).alpha_3
                            processed.append(ip)
                            stats[alpha_3_code] += 1 
                    elif response.status_code == requests.codes.forbidden:
                        raise Exception("You've used up the allotted 15,000 API requests per hour.")

    with open('results.csv', 'w') as csv_file:
        writer = csv.writer(csv_file)
        for key, value in stats.items():
            writer.writerow([key, value])

    csv_file = os.path.join('./', 'results.csv')
    results = pd.read_csv(csv_file)
    world_countries = os.path.join('./', 'world-countries.json')

    m = folium.Map([40,-35], zoom_start=3)

    folium.Choropleth(
        geo_data=world_countries,
        data=results,
        line_weight=0,
        columns=['Country_Code', 'Connections'],
        key_on='feature.id',
        fill_color='OrRd',
        legend_name='Number of unique IP addresses that attempted login',
        nan_fill_color='white',
        bins=[0,10,25,50,100,150,200],
    ).add_to(m)

    m.save('index.html')
    webbrowser.open('file://' + os.path.realpath('index.html'))

main()