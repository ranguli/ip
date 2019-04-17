import socket

import requests
import pycountry
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from database_structure import AccessAttempts, KnownVPNServers, Base

api_url = 'https://freegeoip.app/json/'
vpn_list = 'hostname.sample.txt'
log_file = 'auth.log'

engine = create_engine('sqlite:///db.sqlite')
Base.metadata.bind = engine

DBSession = sessionmaker(bind=engine)
session = DBSession()

def get_country_codes(ip):
    """ Returns country_codes list, [0] is alpha 2 and [1] is alpha 3 """
    country_codes = []
    response = requests.request("GET", str(api_url + ip))
    if response.status_code == requests.codes.ok:
        country = response.json().get('country_name')
        alpha_2_code = response.json().get('country_code')
        if alpha_2_code == '': # Sometimes there are spooky IP's with no country code
            country_codes.append(None)
            country_codes.append(None)
            country_codes.append(None)
        else:
            country_codes.append(country)
            country_codes.append(alpha_2_code)
            country_codes.append(pycountry.countries.get(alpha_2=alpha_2_code).alpha_3)
    elif response.status_code == requests.codes.forbidden:
        raise Exception("You've used up the allotted 15,000 API requests per hour.")
    return country_codes

def vpn_to_db():
    with open(vpn_list, "r") as f:
        for host in f:
            if "#" not in host and host != '':
                try: 
                    ip = socket.gethostbyname(host.rstrip())
                except:
                    ip = None 
                result = get_country_codes(ip)
                country = result[0]
                alpha_2 = result[1]
                alpha_3 = result[2]

                new_vpn = KnownVPNServers(ip_addr=ip, hostname=host, country=country, country_alpha_2=alpha_2, country_alpha_3=alpha_3)
                session.add(new_vpn)
        session.commit()

def log_to_db():
    processed = []
    with open(log_file, "r") as f:
        for line in f:
            if "Disconnected" in line: # Grab the line where the bot disconnects
                ip = line.split()[10]
                if not any(c.isalpha() for c in ip) and ip not in processed: # Check that the IP we grabbed is good
                    try: 
                        print("Attempting to resolve hostname of " + str(ip) + "...")
                        host = socket.gethostbyaddr(ip)
                    except:
                        host = None
                    new_attempt = AccessAttempts(ip_addr=ip, hostname=host)
                    session.add(new_attempt)
                    processed.append(ip)

    session.commit()


# Old mapping code, we likely won't be using this anymore.
"""
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
"""


vpn_to_db()
#import_vpn()
#dump_log()