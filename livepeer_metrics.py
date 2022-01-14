#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec 23 16:35:09 2021
TO DO:
    intermitent issue where 'database locked' error is raised
@author: ghost
"""
import logging
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

file = logging.FileHandler('app.log')
file.setLevel(logging.DEBUG)
fileformat = logging.Formatter("%(asctime)s:%(levelname)s:%(message)s",datefmt="%H:%M:%S")
file.setFormatter(fileformat)

stream = logging.StreamHandler()
stream.setLevel(logging.DEBUG)
streamformat = logging.Formatter("%(asctime)s:%(levelname)s:%(message)s")
stream.setFormatter(streamformat)

log.addHandler(file)
log.addHandler(stream)
print('Application has started')


from flask import Flask, json, request, jsonify
#import requests
#import json
#import pandas as pd
#import re
#import socket
#import folium
#import ast
#from http.server import HTTPServer, BaseHTTPRequestHandler
#from io import BytesIO
from threading import Thread
import time
import common
import os
from web3.auto import w3
from eth_account.messages import encode_defunct
import multiprocessing
from multiprocessing import Process
import gunicorn.app.base
from os import kill, getpid
from signal import SIGTERM


import functools
print = functools.partial(print, flush=True)

import importlib
importlib.reload(common)
    
def number_of_workers():
    #return (multiprocessing.cpu_count() * 2) + 1
    return 4
    
class StandaloneApplication(gunicorn.app.base.BaseApplication):

    def __init__(self, app, options=None):
        self.options = options or {}
        self.application = app
        super().__init__()

    def load_config(self):
        config = {key: value for key, value in self.options.items()
                  if key in self.cfg.settings and value is not None}
        for key, value in config.items():
            self.cfg.set(key.lower(), value)

    def load(self):
        return self.application

options = {
    'bind': '%s:%s' % ('0.0.0.0', '5000'),
    'workers': number_of_workers(),
}

def wsgi_tasks():
    api = Flask(__name__)
    
    @api.route('/authenticate', methods=['POST'])
    def authenticate():
        print('test info')
        print('test debug')
        print('test warn')
        print('test error')
        print('test critical')
        print('Received authenticate api request')
        global db
        #db_flask = common.LpMetricsDb('lpmetrics.db')
        print(request.json)
        data = request.json
        address, authenticated = verify_signature(data['message'], data['signature'], db.orch_addresses)
        print(address)
        if authenticated:
            return 'Success'
        else:
            return 'Failure'
        
    
    @api.route('/metrics', methods=['POST'])
    def get_metrics():
        print('Received get_metrics api request')
        global db
        data = request.json
        address, authenticated = verify_signature(data['message'], data['signature'], db.orch_addresses)
        if authenticated:
            data = db.serve_local_metrics()
            print('get_metrics served successfully')
            return data
        else:
            return 'Authentication unsuccessful'
        
    @api.route('/local_metrics', methods=['GET'])
    def get_local_metrics():
        print('Received get_local_metrics api request')
        global db
        global configs
        
        if request.remote_addr in configs['no_auth_ips']:
            data = db.serve_local_metrics()
            print('get_local_metrics served successfully')
            return data
        else:
            return 'You are not authorized'
        
    
    @api.route('/all_metrics', methods=['GET'])
    def get_all_metrics():
        print('Received get_all_metrics api request')
        global db
        global configs
        
        if request.remote_addr in configs['no_auth_ips']:
            data = db.serve_all_metrics()
            print('get_all_metrics served successfully')
            return data
        else:
            return 'You are not authorized'    
    
    @api.route('/geo_file', methods=['GET'])
    def get_geo():
        with open('geomap.json') as f:
            data = json.load(f)
            return jsonify(data)
        
    @api.route('/geo', methods=['POST'])
    def get_orchgeo():
        
        print('Received getGeoMetrics api request')
        global db
        data = request.json
        address, authenticated = verify_signature(data['message'], data['signature'], db.orch_addresses)
        if authenticated:
            data = db.sql_to_json('SELECT * FROM orch_geo_local')
            print('getGeoMetrics served successfully')
            return jsonify(data)
        else:
            return 'Authentication unsuccessful'

    @api.route('/geo_local', methods=['GET'])
    def get_orch_geo_local_metrics():
        print('Received getLocalGeoMetrics api request')
        global db
        global configs
        
        if request.remote_addr in configs['no_auth_ips']:
            data = db.sql_to_json('SELECT * FROM orch_geo_local')
            print('getLocalGeoMetrics served successfully')
            return jsonify(data)
        else:
            return 'You are not authorized'
        
    @api.route('/geo_prometheus', methods=['GET'])
    def get_orch_geo_prometheus_metrics():
        print('Received getPrometheusGeoMetrics api request')
        global db
        global configs
        
        if request.remote_addr in configs['no_auth_ips']:
            data = db.sql_to_json('SELECT * FROM orch_geo_global')
            print('getPrometheusGeoMetrics served successfully')
            return jsonify(data)
        else:
            return 'You are not authorized'  

    @api.route('/metrics_json', methods=['GET'])
    def get_metrics_json():
        print('Received getLocalGeoMetrics api request')
        global db
        global configs
        
        if request.remote_addr in configs['no_auth_ips']:
            df_data = db.getGeoWithMetrics()
            df_data.fillna('',inplace=True)
            data = df_data.to_dict(orient='records')
            return jsonify(data)
        else:
            return 'You are not authorized'
        
    StandaloneApplication(api, options).run()


# variables that are accessible from anywhere
configs = {}
ignition = True
configs['participating_orchestrators'] = []
configs['no_auth_ips'] = []

print('Loading configuration file')
try:
    with open('app.conf') as f:
        lines = f.read().splitlines()
        for line in lines:
            if  ':' in line:
                if line[0] != '#':
                    line.replace("\n", '')
                    key_value = line.split(':')
                    if key_value[0] == 'participating_orchestrator':
                        value = line.replace(key_value[0]+':','')
                        configs['participating_orchestrators'].append(json.loads(value))
                    elif key_value[0] == 'no_auth_ips':
                        configs['no_auth_ips'].append(key_value[1])
                    else:
                        configs[key_value[0]] = key_value[1]
        
        if configs.get('exclude_metrics') == None: configs['exclude_metrics'] = []
except Exception as e:
    print('Error loading configuration file - likely invalid configs: %s',e)

print('Instantiate a LPMetricsDB object')
db = common.LpMetricsDb('lpmetrics.db',configs)

print('Instantiate flask app')



def background_tasks():
    global configs
    global ignitionD
    global db
    global log
    
    while True:
        print('background task loop iteration')
        db.update_local_metrics_staging_in_db()
        db.update_local_metrics_in_db()
        db.update_remote_metrics_staging_in_db()
        db.update_remote_metrics_in_db()
        print('background task completed')
        time.sleep(5)


def verify_signature(message, signature, addresses):
    try:
        msg = encode_defunct(text=message)
        address = w3.eth.account.recover_message(msg,signature=signature)
        address = str(address).lower()
        auth = address in addresses
        return address, auth
    except Exception as e:
        print('verify_signature function has failed: %s',e)
        return None, False
    





if __name__ == '__main__':
    print('starting background thread')
    

    '''
    bg_thread = Thread(target=background_tasks)
    bg_thread.daemon = True
    bg_thread.start()
    '''
    bg = multiprocessing.Process(target=wsgi_tasks)
    bg.daemon = True
    bg.start()
    #bg_thread.join()

    print('Starting api thread')
    
    background_tasks()
    #StandaloneApplication(api, options).run()
    #os.system('gunicorn --bind 0.0.0.0:5000 livepeer_metrics:api' )
    #api.run()

    #ignition = False
    
    
    print('All threads have terminated')
