#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec 23 16:35:09 2021

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
log.info('Application has started')


from flask import Flask, json, request
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


import functools
print = functools.partial(print, flush=True)

import importlib
importlib.reload(common)


# variables that are accessible from anywhere
configs = {}
ignition = True

log.debug('Loading configuration file')
try:
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
except Exception as e:
    log.fatal('Error loading configuration file - likely invalid configs: %s',e)

log.debug('Instantiate a LPMetricsDB object')
db = common.LpMetricsDb('lpmetrics.db',configs)

log.debug('Instantiate flask app')
api = Flask(__name__)


def background_tasks():
    global configs
    global ignitionD
    global db
    global log
    
    
    
    while ignition:
        log.debug('background task loop iteration')
        db.update_local_metrics_staging_in_db()
        db.update_local_metrics_in_db()
        db.update_remote_metrics_staging_in_db()
        db.update_remote_metrics_in_db()
        time.sleep(10)


def verify_signature(message, signature, addresses):
    try:
        msg = encode_defunct(text=message)
        address = w3.eth.account.recover_message(msg,signature=signature)
        address = str(address).lower()
        auth = address in addresses
        return address, auth
    except Exception as e:
        log.error('verify_signature function has failed: %s',e)
        return None, False
    

@api.route('/authenticate', methods=['POST'])
def authenticate():
    log.info('test info')
    log.debug('test debug')
    log.warn('test warn')
    log.error('test error')
    log.critical('test critical')
    log.debug('Received authenticate api request')
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
    log.debug('Received get_metrics api request')
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
    log.debug('Received get_local_metrics api request')
    global db
    global configs
    
    if request.remote_addr in configs['no_auth_ips']:
        data = db.serve_local_metrics()
        return data
    else:
        return 'You are not authorized'

@api.route('/all_metrics', methods=['GET'])
def get_all_metrics():
    log.debug('Received get_all_metrics api request')
    global db
    global configs
    
    if request.remote_addr in configs['no_auth_ips']:
        data = db.serve_all_metrics()
        return data
    else:
        return 'You are not authorized'

if __name__ == '__main__':
    log.debug('starting background thread')
    bg_thread = Thread(target=background_tasks)
    bg_thread.daemon = True
    bg_thread.start()
    log.debug('Starting api thread')
    os.system('gunicorn --bind 0.0.0.0:5000 livepeer_metrics:api' )
    #api.run()

    ignition = False
    log.info('All threads have terminated')