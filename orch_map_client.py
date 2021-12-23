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
import ast
from http.server import HTTPServer, BaseHTTPRequestHandler
from io import BytesIO


def parse_ip(url):
    try:
        host = re.search("https://(.*?)\:", url).group(1)
        ip = socket.gethostbyname(host)
    except:
        ip = None    
    return ip

def get_ip_loc(ip,n):
    try:
        url = 'http://ipinfo.io/{}/?token=5a06a4a8675de2'.format(ip)
        response = requests.get(url)
        rjs = response.json()
        loc = rjs['loc'].split(',')
        
        d_loc = {'lat':float(loc[0]), 'lon':float(loc[1])}
        return d_loc[n]
    except:
        return None
    
def process_request():
    cli_request = requests.get("http://localhost:7935/registeredOrchestrators", verify=False)

    df = pd.DataFrame(cli_request.json())
    
    
    df['IP'] = df['ServiceURI'].apply(parse_ip)
    df['LAT'] = df['IP'].apply(get_ip_loc, args=('lat',))
    df['LON'] = df['IP'].apply(get_ip_loc, args=('lon',))
    
    return df

class SimpleHTTPRequestHandler(BaseHTTPRequestHandler):
    
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type','application/json')
        self.end_headers()
        df = process_request()
        data = json.dumps(df.to_json(orient='records'))
        self.wfile.write(data.encode('utf-8'))
        
def http_server(mode = 'client'):
    
    if mode == 'client':
        httpd = HTTPServer(('', 6000), SimpleHTTPRequestHandler)

        while True:
            httpd.handle_request()

    if mode == 'server':
        
        df_list=[]
        df_list.append(process_request())
        
        print('getting data from chicago')
        get_chicago = requests.get('http://107.191.48.167:6000', verify=False)
        df_chicago = pd.DataFrame(json.loads(get_chicago.json()))
        df_list.append(df_chicago)
        
        print('getting data from frankfurt')
        get_frank = requests.get('http://frankfurt.ftkuhnsman.com:6000', verify=False)
        df_frank = pd.DataFrame(json.loads(get_frank.json()))
        df_list.append(df_frank)
        
        print('done')
        
        return pd.concat(df_list)
    
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
if __name__ == '__main__':
    df = http_server(mode='client')
    

#%%
    df.dropna(how='any',inplace=True)
    df.drop_duplicates(ignore_index=True, inplace=True)
#%%
    m = folium.Map(prefer_canvas=True)

    #use df.apply(,axis=1) to "iterate" through every row in your dataframe
    df.apply(plotDot, axis = 1)


    #Set the zoom to the maximum possible
    m.fit_bounds(m.get_bounds())

    #Save the map to an HTML file
    m.save('orch_map.html')
