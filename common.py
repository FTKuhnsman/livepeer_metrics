# -*- coding: utf-8 -*-
"""
Created on Fri Jun 11 19:19:22 2021

TO DO:
1. this does not yet consider instance profiles defined in roles
2. attached or inline policies also grant users the ability to assume roles... need to capture this in the roles results tab

saved a working version on 11.6.2021 before changing the way this handles '*' characters

@author: jkuhnsman
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

import json
import pandas as pd
import re
import sqlite3
from sqlite3 import Error
import requests
import functools
import hashlib
import socket

print = functools.partial(print, flush=True)
#from datetime import datetime
#from pypika import Query, Table, Field

class Database:
    def __init__(self,_db_identifier):
        self.conn = None
        
        self.db_identifier = _db_identifier
            
        self.conn = self.create_connection(self.db_identifier)
        
    def create_connection(self,name):
        conn = None
        try:
            conn = sqlite3.connect(name, check_same_thread=False)
            
        except Error as e:
            print(e)
        return conn
    
    def execute_sql(self,_sql_statement):
        if self.conn is not None:
            try:
                c = self.conn.cursor()
                c.execute(_sql_statement)
                self.conn.commit()
                return c.fetchall()
            except Error as e:
                print(e)
                print(_sql_statement)
            
    def sql_to_json(self,_sql_statement):
        if self.conn is not None:
            try:
                c = self.create_connection(self.db_identifier)
                c.row_factory = sqlite3.Row
                rows = c.execute(_sql_statement).fetchall()
                c.commit()
                c.close()
                data=[dict(ix) for ix in rows]
                return data
            except Error as e:
                print(e)
                print(_sql_statement)

    def execmany_sql(self,_sql_statement,_data):
        if self.conn is not None:
            try:
                c = self.conn.cursor()
                c.executemany(_sql_statement,_data)
                self.conn.commit()
                return c.fetchall()
            except Error as e:
                print(e)
                print(_sql_statement)                
    
            
    def make_list(self,_l):
        if not isinstance(_l, list):
            return [_l]
        return _l

    def sql_to_df(self, _sql_statement):
        if self.conn is not None:
            try:
                return pd.read_sql(_sql_statement, con=self.conn)
            except Error as e:
                print(e)
                
    def get_tables(self):
        tables = self.execute_sql("SELECT name FROM sqlite_master WHERE type='table';")
        tlist = [x[0] for x in tables]
        return tlist

class LpMetricsDb(Database):
    
    def __init__(self,_db_identifier=None,_configs=None):
        Database.__init__(self, _db_identifier)
        self.static_statements = self.get_static_statements()
        self.configs = _configs
        
        self.initialize_db()
        self.update_orch_geo_local_table()
        print('LPMetricsDB instance init function complete')
        
        
    def initialize_db(self):
        tables = self.get_tables()
        if not 'active_orchs' in tables:
            print('active_orchs table does not exist... creating table')
            self.init_active_orchs()
        elif self.execute_sql('SELECT * FROM active_orchs') == []:
            print('active_orchs table is empty... populating table')
            self.init_active_orchs()
        
        self.init_metrics_tables()


    def init_active_orchs(self):
        self.execute_sql("DROP TABLE IF EXISTS active_orchs")
        self.execute_sql(self.static_statements['create_active_orchs_table'])
        orchs = self.get_active_orchs_from_cli()
        
        for o in orchs:            
            sql_insert = """INSERT INTO active_orchs VALUES (null,'{address}','{delegated_stake}','{fee_share}','{reward_cut}','{service_uri}')""".format(
                address=o['Address'],
                delegated_stake=o['DelegatedStake'],
                fee_share=o['FeeShare'],
                reward_cut=o['RewardCut'],
                service_uri=o['ServiceURI'])
            #insert records
            self.execute_sql(sql_insert)
        print('active_orchs table created')
    
    def init_metrics_tables(self):
        self.execute_sql('DROP TABLE IF EXISTS metrics')
        self.execute_sql(self.static_statements['create_metrics_table'])
        print('metrics table reset')
        self.execute_sql('DROP TABLE IF EXISTS metrics_staging')
        self.execute_sql(self.static_statements['create_metrics_staging_table'])
        print('metrics_staging table reset')
        self.execute_sql('DROP TABLE IF EXISTS local_metrics')
        self.execute_sql(self.static_statements['create_local_metrics_table'])
        print('local_metrics table reset')
        self.execute_sql('DROP TABLE IF EXISTS local_metrics_staging')
        self.execute_sql(self.static_statements['create_local_metrics_staging_table'])
        print('local_metrics_staging table reset')
        
    def get_active_orchs_from_cli(self):
        print('retrieving active orchs from cli')
        r = requests.get('http://localhost:7935/registeredOrchestrators', verify=False)
        return r.json()
    
    def parse_ip(self,url):
        try:
            host = re.search("https://(.*?)\:", url).group(1)
            ip = socket.gethostbyname(host)
        except:
            ip = None    
        return ip
    
    def get_ip_loc(self,ip):
        try:
            url = 'http://ipinfo.io/{}'.format(ip)
            response = requests.get(url)
            rjs = response.json()
            loc = rjs['loc'].split(',')
            
            d_loc = {'lat':float(loc[0]), 'lon':float(loc[1])}
            return d_loc
        except:
            return None
    
    def get_orch_geo_local(self):
        orchs = self.sql_to_json('SELECT * FROM active_orchs')
        for o in orchs:
            ip = self.parse_ip(o['service_uri'])
            print(ip)
            coord = self.get_ip_loc(ip)
            try:
                o['lat'] = coord['lat']
                o['lon'] = coord['lon']
                o['ip'] = ip
            except:
                o['lat'] = None
                o['lon'] = None
                o['ip'] = ip
        return orchs
    
    def update_orch_geo_local_table(self):
        orchs = self.get_orch_geo_local()
        self.execute_sql('DROP TABLE IF EXISTS orch_geo_local')
        self.execute_sql(self.static_statements['create_orch_geo_local_table'])
        
        
        
        for o in orchs:            
            sql_insert = """INSERT INTO orch_geo_local VALUES (null,'{address}','{delegated_stake}','{fee_share}','{reward_cut}','{service_uri}','{lat}','{lon}','{count}','{ip}')""".format(
                address=o['address'][2:],
                delegated_stake=o['delegated_stake'],
                fee_share=o['fee_share'],
                reward_cut=o['reward_cut'],
                service_uri=o['service_uri'],
                lat=o['lat'],
                lon=o['lon'],
                count=1,
                ip=o['ip'])
            #insert records
            if o['lat'] == None: continue
            self.execute_sql(sql_insert)
        print('orch_geo_local table created')        
        
            
    
    @property
    def orch_addresses(self):
        orchs = self.sql_to_df('SELECT * FROM active_orchs')
        orch_list = orchs['address'].tolist()
        return orch_list
        
    def get_static_statements(self):
        statements = {}
        
        #active orchs table
        __sql_create_active_orch_table = """CREATE TABLE active_orchs (
                                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                                        address text NOT NULL,
                                        delegated_stake integer NOT NULL,
                                        fee_share integer NOT NULL,
                                        reward_cut integer NOT NULL,
                                        service_uri text
                                    );"""
        statements['create_active_orchs_table'] = __sql_create_active_orch_table
        
        __sql_create_orch_geo_local_table = """CREATE TABLE orch_geo_local (
                                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                                        address text NOT NULL,
                                        delegated_stake integer NOT NULL,
                                        fee_share integer NOT NULL,
                                        reward_cut integer NOT NULL,
                                        service_uri text,
                                        lat text,
                                        lon text,
                                        count,
                                        ip
                                    );"""
        statements['create_orch_geo_local_table'] = __sql_create_orch_geo_local_table
        
        __sql_create_orch_geo_global_table = """CREATE TABLE orch_geo_global (
                                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                                        address text NOT NULL,
                                        delegated_stake integer NOT NULL,
                                        fee_share integer NOT NULL,
                                        reward_cut integer NOT NULL,
                                        service_uri text,
                                        lat text,
                                        lon text,
                                        count,
                                        ip
                                    );"""
        statements['create_orch_geo_global_table'] = __sql_create_orch_geo_global_table

        #metrics table
        __sql_create_metrics_table = """CREATE TABLE IF NOT EXISTS metrics (
                                        id text NOT NULL PRIMARY KEY,
                                        metric text NOT NULL,
                                        tags text NOT NULL,
                                        value text NOT NULL
                                    );"""
        statements['create_metrics_table'] = __sql_create_metrics_table
        
        #metrics staging table
        __sql_create_metrics_staging_table = """CREATE TABLE IF NOT EXISTS metrics_staging (
                                        id text NOT NULL PRIMARY KEY,
                                        metric text NOT NULL,
                                        tags text NOT NULL,
                                        value text NOT NULL
                                    );"""
        statements['create_metrics_staging_table'] = __sql_create_metrics_staging_table
        
        #local_metrics table
        __sql_create_local_metrics_table = """CREATE TABLE IF NOT EXISTS local_metrics (
                                        id text NOT NULL PRIMARY KEY,
                                        metric text NOT NULL,
                                        tags text NOT NULL,
                                        value text NOT NULL
                                    );"""
        statements['create_local_metrics_table'] = __sql_create_local_metrics_table
        
        #local metrics staging table
        __sql_create_local_metrics_staging_table = """CREATE TABLE IF NOT EXISTS local_metrics_staging (
                                        id text NOT NULL PRIMARY KEY,
                                        metric text NOT NULL,
                                        tags text NOT NULL,
                                        value text NOT NULL
                                    );"""
        statements['create_local_metrics_staging_table'] = __sql_create_local_metrics_staging_table
        
        return statements
    
    def getGeoMetrics(self, ip, port, eth, message=None, signature=None, return_r=False):

        try:
            print('getGeoMetrics function has been called')
            url = 'http://'+ip+':'+port+'/geo'
            print('getGeoMetrics: url = %s',url)
            if message == None or signature == None:
                print('getGeoMetrics: requesting metrics without authentication')
                r = requests.get(url, verify=False, timeout=2)
                print('getGeoMetrics: response status code %s',r.status_code)
                
            else:
                print('getGeoMetrics: requesting metrics with authentication')
                r = requests.post(url, json={'message':message,'signature':signature}, verify=False, timeout=2)
                print('getGeoMetrics: response status code %s',r.status_code)
                #print(r.content)
                return r.json()
            
            '''
            raw = r.text
            raw_split = raw.split('\n')
    
            metrics = []
            
            for m in raw_split:
                if (not '#' in m) & ('livepeer' in m):
                    metrics.append(m)
            
            
            
            for m in metrics:
                l = re.split('{|}',m)
                metric = str(l[0])
                tags = str(l[1])
                value = str(l[-1]).strip()
                
                tags_dict = self.split_with_quotes(tags)
                #print(tags_dict)
                tags_dict['ip'] = ip
                tags_dict['eth'] = eth
                
                tags_string = json.dumps(tags_dict)
                
                ID = hashlib.md5(str.encode(metric+tags_string)).hexdigest()
                
                metrics_parsed.append({'id':ID,'metric':metric,'tags':tags_string,'value':value})
            '''
        except Exception as e:
            print('getGeoMetrics function failed: %s', e)
            
        return None
        

        
    def getMetrics(self, ip, port, eth, message=None, signature=None, return_r=False):
        metrics_parsed = []
        try:
            print('getMetrics function has been called')
            url = 'http://'+ip+':'+port+'/metrics'
            print('getMetrics: url = %s',url)
            if message == None or signature == None:
                print('getMetrics: requesting metrics without authentication')
                r = requests.get(url, verify=False, timeout=2)
                print('getMetrics: response status code %s',r.status_code)
                
            else:
                print('getMetrics: requesting metrics with authentication')
                r = requests.post(url, json={'message':message,'signature':signature}, verify=False, timeout=2)
                print('getMetrics: response status code %s',r.status_code)
                #print(r.content)
                
            raw = r.text
            raw_split = raw.split('\n')
    
            metrics = []
            
            for m in raw_split:
                if (not '#' in m) & ('livepeer' in m):
                    metrics.append(m)
            
            
            
            for m in metrics:
                l = re.split('{|}',m)
                metric = str(l[0])
                tags = str(l[1])
                value = str(l[-1]).strip()
                
                tags_dict = self.split_with_quotes(tags)
                #print(tags_dict)
                tags_dict['ip'] = ip
                tags_dict['eth'] = eth
                
                tags_string = json.dumps(tags_dict)
                
                ID = hashlib.md5(str.encode(metric+tags_string)).hexdigest()
                
                metrics_parsed.append({'id':ID,'metric':metric,'tags':tags_string,'value':value})
        except Exception as e:
            print('getMetrics function failed: %s', e)
            return None
        
        if return_r:    
            return metrics_parsed, r
        else:
            return metrics_parsed

    def split_with_quotes(self, infile):
    
        split = 0
        quote = False
        tag_list = []
        tag_dict = {}
        for i in range(0,len(infile)):
            if infile[i] == '"':
                quote = ~quote
                
            if ((infile[i] == ',') or (i == len(infile)-1)) and (quote == False):
                tag_list.append(infile[split:i])
                split = i + 1
                
        for i in tag_list:
            x = i.replace('"','')
            tag = x.split('=')
            tag_dict[tag[0]] = tag[1]
        
        return tag_dict
    
    def update_geo_data_in_db(self):
        print('update geo staging table')
        self.execute_sql('DROP TABLE IF EXISTS orch_geo_global')
        self.execute_sql(self.static_statements['create_orch_geo_global_table'])
        
        orch_geo_list = []
        
        
        
        for orch in self.configs['participating_orchestrators']:
            geo = self.getGeoMetrics(orch['ip'],orch['port'],orch['eth'],message=self.configs['message'],signature=self.configs['signature'])
            if geo != None:
                orch_geo_list.append(geo)
            else:
                print('failed retrieving geo stats from %s',orch['ip'])
        
        if orch_geo_list != []:
            geo = sum(orch_geo_list, [])
                
            _sql = """INSERT INTO orch_geo_global (id,address,delegated_stake,fee_share,reward_cut,service_uri,lat,lon,count,ip)
                        VALUES(?,?,?,?,?,?,?,?,?,?)"""

            _data = [tuple(dic.values()) for dic in geo]
            self.execmany_sql(_sql,_data)
            print('remote metrics staging update complete')
            return metric_list
        else:
            print('failed writing data to remote metrics staging')
            return metric_list 
    
    def update_local_metrics_staging_in_db(self):
        print('update local metrics staging table')
        self.execute_sql('DROP TABLE IF EXISTS local_metrics_staging')
        self.execute_sql(self.static_statements['create_local_metrics_staging_table'])
        
        try:
            metrics = self.getMetrics(self.configs['local_orchestrator']['ip'],self.configs['local_orchestrator']['port'],self.configs['local_orchestrator']['eth'])
            
    
            
            _sql = """INSERT INTO local_metrics_staging (id,metric,tags,value)
                        VALUES(?,?,?,?)"""
            
            _data = [tuple(dic.values()) for dic in metrics]
            self.execmany_sql(_sql,_data)
            print('local metrics staging update complete')
        except:
            print('failed local metrics staging update')
        
    def update_remote_metrics_staging_in_db(self):
        print('update remote metrics staging table')
        self.execute_sql('DROP TABLE IF EXISTS metrics_staging')
        self.execute_sql(self.static_statements['create_metrics_staging_table'])
        
        metric_list = []
        for orch in self.configs['participating_orchestrators']:
            metrics = self.getMetrics(orch['ip'],orch['port'],orch['eth'],message=self.configs['message'],signature=self.configs['signature'])
            if metrics != None:
                metric_list.append(metrics)
            else:
                print('failed retrieving metrics from %s',orch['ip'])
        
        if metric_list != []:
            metrics = sum(metric_list, [])
                
            _sql = """INSERT INTO metrics_staging (id,metric,tags,value)
                        VALUES(?,?,?,?)"""
            
            _data = [tuple(dic.values()) for dic in metrics]
            self.execmany_sql(_sql,_data)
            print('remote metrics staging update complete')
            return metric_list
        else:
            print('failed writing data to remote metrics staging')
            return metric_list
        
    def update_local_metrics_in_db(self):
        print('syncing local metrics staging to local metrics table')
        try:
            _sql1 = """INSERT INTO local_metrics
                        SELECT * FROM local_metrics_staging
                        WHERE id NOT IN (SELECT id from local_metrics);"""
            _sql2 = """UPDATE local_metrics
                        SET value = (SELECT value FROM local_metrics_staging WHERE id = local_metrics.id)
                        WHERE value <> (SELECT value FROM local_metrics_staging WHERE id = local_metrics.id);"""
            _sql3 = """DELETE FROM local_metrics WHERE id NOT IN (SELECT id from local_metrics_staging);"""
            
            self.execute_sql(_sql1)
            self.execute_sql(_sql2)
            self.execute_sql(_sql3)
            print('local metrics syncing complete')
        except Exception as e:
            print('failed syncing local metrics from staging: %s',e)
        
    def update_remote_metrics_in_db(self):
        print('syncing all staging to metrics table')
        try:
            _sql1l = """INSERT INTO metrics
                        SELECT * FROM local_metrics_staging
                        WHERE id NOT IN (SELECT id from metrics);"""
            _sql1r = """INSERT INTO metrics
                        SELECT * FROM metrics_staging
                        WHERE id NOT IN (SELECT id from metrics);"""
            _sql2l = """UPDATE metrics
                        SET value = (SELECT value FROM local_metrics_staging WHERE id = metrics.id)
                        WHERE value <> (SELECT value FROM local_metrics_staging WHERE id = metrics.id);"""
            _sql2r = """UPDATE metrics
                        SET value = (SELECT value FROM metrics_staging WHERE id = metrics.id)
                        WHERE value <> (SELECT value FROM metrics_staging WHERE id = metrics.id);"""
            _sql3 = """DELETE FROM metrics WHERE id NOT IN (SELECT id from local_metrics_staging) AND id NOT IN (SELECT id from metrics_staging);"""
            
            #print('1l')
            self.execute_sql(_sql1l)
            #print('1r')
            self.execute_sql(_sql1r)
            #print('2l')
            self.execute_sql(_sql2l)
            #print('2r')
            self.execute_sql(_sql2r)
            #print('3')
            self.execute_sql(_sql3)
            print('all metrics sycing complete')
        except Exception as e:
            print('failed syncing all staging metrics to metrics table: %s',e)
        
    def serve_local_metrics(self):
        metrics = self.sql_to_json('SELECT * FROM local_metrics')
        rows = []
        
        for m in metrics:
            if m['metric'] in self.configs['exclude_metrics']:
                continue
            
            tag = json.loads(m['tags'])
            tag_str = '{'
            for key, val in tag.items():
                tag_str += (key+'='+'"'+val+'",')
            tag_str = tag_str[:-1]
            tag_str += '}'
            
            row = m['metric']+tag_str+' '+m['value']
            rows.append(row)
        
        data = '\n'.join(rows)
        
        return data

    def serve_all_metrics(self):
        metrics = self.sql_to_json('SELECT * FROM metrics')
        rows = []
        
        for m in metrics:
            if m['metric'] in self.configs['exclude_metrics']:
                continue
            
            tag = json.loads(m['tags'])
            tag_str = '{'
            for key, val in tag.items():
                tag_str += (key+'='+'"'+val+'",')
            tag_str = tag_str[:-1]
            tag_str += '}'
            
            row = 'community_metrics_'+m['metric']+tag_str+' '+m['value']
            rows.append(row)
        
        data = '\n'.join(rows)
        
        return data
        
if __name__ == '__main__':
    configs = {}
    ignition = True
    
    print('Loading configuration file')
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
        print('Error loading configuration file - likely invalid configs: %s',e)
    
    print('Instantiate a LPMetricsDB object')
    db = LpMetricsDb('lpmetrics.db',configs)  
