#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec 23 18:37:46 2021

@author: ghost
"""

import requests
import re
from csv import reader
import csv
ip = 'chicago.ftkuhnsman.com'
port = '7935'
url = 'http://'+ip+':'+port+'/metrics'
r = requests.get(url, verify=False)
raw = r.text
raw_split = raw.split('\n')

metrics = []

for m in raw_split:
    if (not '#' in m) & ('livepeer' in m):
        metrics.append(m)

metrics_parsed = {}

for m in metrics:
    l = re.split('{|}',m)
    metric = l[0]
    tags = l[1]
    value = l[-1]
    
    tags_dict = split_with_quotes(tags)
    
    metrics_parsed[l[0]] = {'tags':tags_dict,'value':float(str(l[-1]).strip())}

def split_with_quotes(infile):

    split = 0
    quote = False
    tag_list = []
    tag_dict = {}
    for i in range(0,len(infile)):
        if infile[i] == '"':
            quote = ~quote
            
        if (infile[i] == ',') and (quote == False):
            tag_list.append(infile[split:i])
            split = i + 1
            
    for i in tag_list:
        x = i.replace('"','')
        tag = x.split('=')
        tag_dict[tag[0]] = tag[1]
    
    return tag_dict

infile = metrics_parsed['livepeer_transcode_time_seconds_bucket']['tags']
res = split_with_quotes(infile)

r = requests.post('http://localhost:5000/metrics', json={'signature':'0x2b19bd264b2a23a3c38d62b547be64d295d9446250d7c112d1b657e4020933cf74c457f8811246aad63eb7a7d4a050a8ba6b2de4f8b2090684f6e734ce8ed7951b','message':'livepeer'})
r = requests.get('http://localhost:7935/registeredOrchestrators')
r = requests.post('http://localhost:5000/authenticate', json={'signature':'0x2b19bd264b2a23a3c38d62b547be64d295d9446250d7c112d1b657e4020933cf74c457f8811246aad63eb7a7d4a050a8ba6b2de4f8b2090684f6e734ce8ed7951b','message':'livepeer'})

configs = {}
with open('app.conf') as f:
    lines = f.read().splitlines()
    for line in lines:
        if line[0] != '#':
            line.replace("\n", '')
            key_value = line.split(':')
            configs[key_value[0]] = key_value[1]
            
r = requests.post('http://localhost:5000/metrics', json={'message':configs['message'],'signature':configs['signature']})


ip = 'chicago.ftkuhnsman.com'
port = '7935'
url = 'http://'+ip+':'+port+'/metrics'
r = requests.get(url, verify=False)
raw = r.text
raw_split = raw.split('\n')

metrics = []

for m in raw_split:
    if (not '#' in m) & ('livepeer' in m):
        metrics.append(m)

metrics_parsed = {}

for m in metrics:
    l = re.split('{|}',m)
    metric = l[0]
    tags = l[1]
    value = l[-1]
    '''
    t_raw = []
    for r in re.findall(r'".+?"|[\w-]+', tags):
        t_raw.append(r)

    t_parsed = {}
    if len(t_raw) > 0:
        for x in range(0,len(t_raw),2):
            t_parsed[t_raw[x]] = t_raw[x+1]
    '''
    metrics_parsed[l[0]] = {'tags':tags,'value':float(str(l[-1]).strip())}

infile = [metrics_parsed['livepeer_transcode_time_seconds_bucket']['tags']]
for i in reader(infile, delimiter=',', quotechar='"', skipinitialspace=True):
    for x in i: print(x)
   
infile = metrics_parsed['livepeer_transcode_time_seconds_bucket']['tags']
start = 0
split = 0
quote = False
tag_list = []
for i in range(0,len(infile)):
    if infile[i] == '"': quote == ~quote
        
    if (infile[i] == ',') and (quote == False):
        tag_list.append(infile[split:i])
        split = i
        
    

m_raw = []
for r in re.findall(r',(?=([^\"]*\"[^\"]*\")*[^\"]*$)', infile):
    m_raw.append(r)

tag_parsed = {}
for x in range(0,len(m_raw),2):
    tag_parsed[m_raw[x]] = m_raw[x+1]
    
    
a = infile.split('/,+|"[^"]+"/g')


#%%
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
            configs[key_value[0]] = key_value[1]