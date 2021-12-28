# -*- coding: utf-8 -*-
"""
Created on Thu Dec  2 17:34:19 2021

@author: jkuhnsman
"""

import requests
import json
import pandas as pd
import re
import socket
import folium

chicago = requests.get("http://107.191.48.167:7935/registeredOrchestrators", verify=False)
tampa = requests.get("http://tampa.ftkuhnsman.com:7935/registeredOrchestrators", verify=False)
frankfurt = requests.get("http://tampa.ftkuhnsman.com:7935/registeredOrchestrators", verify=False)

#%%
df_list = []
df_list.append(pd.DataFrame(chicago.json()))
df_list.append(pd.DataFrame(tampa.json()))
df_list.append(pd.DataFrame(frankfurt.json()))

df = pd.concat(df_list)
df.

#%%
def parse_ip(url):
    try:
        host = re.search("https://(.*?)\:", url).group(1)
        ip = socket.gethostbyname(host)
    except:
        ip = None    
    return ip

def get_ip_loc(ip,n):
    try:
        url = 'http://ipinfo.io/{}'.format(ip)
        response = requests.get(url)
        rjs = response.json()
        loc = rjs['loc'].split(',')
        
        d_loc = {'lat':float(loc[0]), 'lon':float(loc[1])}
        return d_loc[n]
    except:
        return None

df['IP'] = df['ServiceURI'].apply(parse_ip)
df['LAT'] = df['IP'].apply(get_ip_loc, args=('lat',))
df['LON'] = df['IP'].apply(get_ip_loc, args=('lon',))

#%%

m = folium.Map(prefer_canvas=True)

def plotDot(point):
    '''input: series that contains a numeric named latitude and a numeric named longitude
    this function creates a CircleMarker and adds it to your this_map'''
    try:
        folium.CircleMarker(location=[point.LAT, point.LON],
                            radius=2,
                            weight=5,
                            tooltip=point.Address).add_to(m)
    except:
        pass

#use df.apply(,axis=1) to "iterate" through every row in your dataframe
df.apply(plotDot, axis = 1)


#Set the zoom to the maximum possible
m.fit_bounds(m.get_bounds())

#Save the map to an HTML file
m.save('orch_map.html')