#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec 23 16:35:09 2021

@author: ghost
"""

from flask import Flask, json, request
import requests
import json
import pandas as pd
import re
import socket
import folium
import ast
from http.server import HTTPServer, BaseHTTPRequestHandler
from io import BytesIO
from threading import Thread
import atexit
import time
import common
import os
from web3.auto import w3
from eth_account.messages import encode_defunct

import functools
print = functools.partial(print, flush=True)

import importlib
importlib.reload(common)

# variables that are accessible from anywhere
configs = {}
ignition = True


with open('app.conf') as f:
    lines = f.read().splitlines()
    for line in lines:
        if line[0] != '#':
            line.replace("\n", '')
            key_value = line.split(':')
            if key_value[0] in ['local_orchestrator','participating_orchestrators']:
                key_value[1] = line.replace(key_value[0]+':','')
                key_value[1] = json.loads(key_value[1])
            if key_value[0] in ['no_auth_ips']:
                key_value[1] = line.replace(key_value[0]+':','')
                key_value[1] = str(key_value[1]).split(',')
            configs[key_value[0]] = key_value[1]
    
    if configs.get('exclude_metrics') == None: configs['exclude_metrics'] = []

db = common.LpMetricsDb('lpmetrics.db',configs)

api = Flask(__name__)

def background_tasks():
    global configs
    global ignitionD
    global db
    
    
    
    while ignition:
        #print('background reset')
        db.update_local_metrics_staging_in_db()
        db.update_local_metrics_in_db()
        time.sleep(10)
        # Do your stuff with commonDataStruct Here

def verify_signature(message, signature, addresses):  
    msg = encode_defunct(text=message)
    address = w3.eth.account.recover_message(msg,signature=signature)
    address = str(address).lower()
    auth = address in addresses
    return address, auth
    

@api.route('/authenticate', methods=['POST'])
def authenticate():
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
    global db
    data = request.json
    address, authenticated = verify_signature(data['message'], data['signature'], db.orch_addresses)
    if authenticated:
        data = db.serve_local_metrics()
        return data
    else:
        return 'Authentication unsuccessful'
    
@api.route('/local_metrics', methods=['GET'])
def get_local_metrics():
    global db
    global configs
    
    if request.remote_addr in configs['no_auth_ips']:
        data = db.serve_local_metrics()
        return data
    else:
        return 'You are not authorized'



if __name__ == '__main__':
    bg_thread = Thread(target=background_tasks)
    bg_thread.daemon = True
    bg_thread.start()
    
    os.system('gunicorn --bind 0.0.0.0:5000 livepeer_metrics:api' )
    #api.run()

    ignition = False
