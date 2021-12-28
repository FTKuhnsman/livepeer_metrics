# -*- coding: utf-8 -*-
"""
Created on Fri Jun 11 19:19:22 2021

TO DO:
1. this does not yet consider instance profiles defined in roles
2. attached or inline policies also grant users the ability to assume roles... need to capture this in the roles results tab

saved a working version on 11.6.2021 before changing the way this handles '*' characters

@author: jkuhnsman
"""

import json
import pandas as pd
import re
import sqlite3
from sqlite3 import Error
import requests
import functools
import hashlib
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
            sql_insert = """INSERT INTO active_orchs VALUES (null,'{address}','{delegated_stake}','{fee_share}','{reward_cut}')""".format(
                address=o['Address'],
                delegated_stake=o['DelegatedStake'],
                fee_share=o['FeeShare'],
                reward_cut=o['RewardCut'])
            #insert records
            self.execute_sql(sql_insert)
        print('active_orchs table created')
    
    def init_metrics_tables(self):
        self.execute_sql('DROP TABLE IF EXISTS metrics')
        self.execute_sql(self.static_statements['create_metrics_table'])
        
        self.execute_sql('DROP TABLE IF EXISTS metrics_staging')
        self.execute_sql(self.static_statements['create_metrics_staging_table'])
        
        self.execute_sql('DROP TABLE IF EXISTS local_metrics')
        self.execute_sql(self.static_statements['create_local_metrics_table'])
        
        self.execute_sql('DROP TABLE IF EXISTS local_metrics_staging')
        self.execute_sql(self.static_statements['create_local_metrics_staging_table'])
        
    def get_active_orchs_from_cli(self):
        r = requests.get('http://localhost:7935/registeredOrchestrators')
        return r.json()
    
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
                                        reward_cut integer NOT NULL
                                    );"""
        statements['create_active_orchs_table'] = __sql_create_active_orch_table

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

    def getMetrics(self, ip, port, eth, message=None, signature=None):

        url = 'http://'+ip+':'+port+'/metrics'
        
        if message == None or signature == None:
            r = requests.get(url, verify=False)
        else:
            r = requests.post(url, json={'message':message,'signature':signature}, verify=False)
            print(r.content)
            
        raw = r.text
        raw_split = raw.split('\n')

        metrics = []
        
        for m in raw_split:
            if (not '#' in m) & ('livepeer' in m):
                metrics.append(m)
        
        metrics_parsed = []
        
        for m in metrics:
            l = re.split('{|}',m)
            metric = str(l[0])
            tags = str(l[1])
            value = str(l[-1]).strip()
            
            tags_dict = self.split_with_quotes(tags)
            tags_dict['ip'] = ip
            tags_dict['eth'] = eth
            
            ID = hashlib.md5(str.encode(metric+tags)).hexdigest()
            
            metrics_parsed.append({'id':ID,'metric':metric,'tags':json.dumps(tags_dict),'value':value})
        
        return metrics_parsed

    def split_with_quotes(self, infile):
    
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
    
    '''
    def get_orch_metrics_dataframe(self,ip,port):
        metrics = self.getMetrics(ip, port)
        df = pd.DataFrame(metrics)
        return df
    '''
    
    def update_local_metrics_staging_in_db(self):
        metrics = self.getMetrics(self.configs['local_orchestrator']['ip'],self.configs['local_orchestrator']['port'],self.configs['local_orchestrator']['eth'])
        
        self.execute_sql('DROP TABLE IF EXISTS local_metrics_staging')
        self.execute_sql(self.static_statements['create_local_metrics_staging_table'])
        
        _sql = """INSERT INTO local_metrics_staging (id,metric,tags,value)
                    VALUES(?,?,?,?)"""
        print('executing')
        
        _data = [tuple(dic.values()) for dic in metrics]
        self.execmany_sql(_sql,_data)
        
    def update_local_metrics_in_db(self):
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
        
'''    
    def __load_data_to_db(self):
        self.__load_managed_policy_actions()
        self.__load_users()
        self.__load_user_group_memberships()
        self.__load_users_managed_policies()
        self.__load_groups()
        self.__load_groups_managed_policies()
        self.__load_managed_policies()
        self.__load_user_inline_policy_actions()
        self.__load_group_inline_policy_actions()
        self.__load_roles()
        self.__load_roles_managed_policies()
        self.__load_role_assumerolepolicydocument()
        self.__load_role_inline_policy_actions()
        
    def __load_managed_policies(self):
        print('__load_managed_policies')
        for policy in self.raw_data['Policies']:
            sql_insert = """INSERT INTO managed_policies VALUES ('{arn}','{attachmentcount}','{createdate}','{defaultversionid}','{isattachable}','{policyid}','{policyname}','{updatedate}')""".format(
                arn=policy['Arn'],
                attachmentcount=policy['AttachmentCount'],
                createdate=policy['CreateDate'],
                defaultversionid=policy['DefaultVersionId'],
                isattachable=policy['IsAttachable'],
                policyid=policy['PolicyId'],
                policyname=policy['PolicyName'],
                updatedate=policy['UpdateDate'])
            #insert records
            self.execute_sql(sql_insert)
        
    def __load_groups_managed_policies(self):
        print('__load_groups_managed_policies')
        for group in self.raw_data['GroupDetailList']:
            #generate query statement to populate 'users' table
            if len(group['AttachedManagedPolicies'])>0:
                for mpolicy in group['AttachedManagedPolicies']:
                    sql_insert = """INSERT INTO groups_managed_policies VALUES (null,'{grouparn}','{groupname}','{policyarn}')""".format(
                        grouparn=group['Arn'],
                        groupname=group['GroupName'],
                        policyarn=mpolicy['PolicyArn'])
                    #insert records
                    self.execute_sql(sql_insert)
                    
    def __load_roles_managed_policies(self):
        print('__load_roles_managed_policies')
        for role in self.raw_data['RoleDetailList']:
            #generate query statement to populate 'users' table
            if len(role['AttachedManagedPolicies'])>0:
                for mpolicy in role['AttachedManagedPolicies']:
                    sql_insert = """INSERT INTO roles_managed_policies VALUES (null,'{rolearn}','{policyarn}')""".format(
                        rolearn=role['Arn'],
                        policyarn=mpolicy['PolicyArn'])
                    #insert records
                    self.execute_sql(sql_insert)

    def __load_groups(self):
        print('__load_groups')
        for group in self.raw_data['GroupDetailList']:
            #generate query statement to populate 'users' table
            sql_insert = """INSERT INTO groups VALUES ('{arn}','{groupname}','{groupid}','{createdate}')""".format(
                arn=group['Arn'],
                groupname=group['GroupName'],
                groupid=group['GroupId'],
                createdate=group['CreateDate'])
            #insert user in table    
            self.execute_sql(sql_insert)
    
    def __load_users_managed_policies(self):
        print('__load_users_managed_policies')
        for user in self.raw_data['UserDetailList']:
            #iterate through attached managed policies and add to a join table
            if len(user['AttachedManagedPolicies'])>0:
                for mpolicy in user['AttachedManagedPolicies']:
                    sql_insert = """INSERT INTO users_managed_policies VALUES (null,'{userarn}','{policyarn}')""".format(
                        userarn=str(user['Arn']),
                        policyarn=mpolicy['PolicyArn'])
                    #insert records
                    self.execute_sql(sql_insert)
        
    def __load_users(self):
        print('__load_users')
        for user in self.raw_data['UserDetailList']:
            #generate query statement to populate 'users' table
            sql_insert = """INSERT INTO users VALUES ('{arn}','{username}','{userid}','{createdate}')""".format(
                arn=user['Arn'],
                username=user['UserName'],
                userid=user['UserId'],
                createdate=user['CreateDate'])
            #insert user in table    
            self.execute_sql(sql_insert)

    def __load_roles(self):
        print('__load_roles')
        for role in self.raw_data['RoleDetailList']:
            #generate query statement to populate 'roles' table
            sql_insert = """INSERT INTO roles VALUES ('{arn}','{rolename}','{roleid}','{createdate}')""".format(
                arn=role['Arn'],
                rolename=role['RoleName'],
                roleid=role['RoleId'],
                createdate=role['CreateDate'])
            #insert user in table    
            self.execute_sql(sql_insert)
            
    def __load_user_group_memberships(self):
        print('__load_user_group_memberships')
        for user in self.raw_data['UserDetailList']:
            #iterate through groups and add to a join table
            if len(user['GroupList'])>0:
                for group in user['GroupList']:
                    sql_insert = """INSERT INTO user_group_membership VALUES (null,'{userarn}','{groupname}')""".format(
                        userarn=str(user['Arn']),
                        groupname=group)
                    #insert records
                    self.execute_sql(sql_insert)
        
    def __load_managed_policy_actions(self):
        print('__load_managed_policy_actions')
        for i, policy in enumerate(self.raw_data['Policies']):
            for version in policy['PolicyVersionList']:
                if version['IsDefaultVersion']:
                    for statement_list in self.make_list(version['Document']):
                        for statement in self.make_list(statement_list['Statement']):
                            for action in self.make_list(statement.get('Action',[])):
                                try:
                                    action_raw=action
                                    action_split = str(action_raw).split(':')
                                    if len(action_split) == 1:
                                        action_split.append('*')
                                    
                                    
                                    resource_list = self.make_list(statement.get('Resource',[]))
                                    resource = "\n".join(resource_list)
                                    
                                    if len(resource_list)>0:
                                        if 'Condition' in statement:
                                            c = json.dumps(statement['Condition'])
                                        else:
                                            c = ""
    
                                        sql_insert = """INSERT INTO managed_policy_actions VALUES (null,'{_policyarn}','{_versionid}','{_actiontype}','{_action_raw}','{_service}','{_action}','{_action_formatted}','{_resourcetype}','{_resource}','{_effect}','{_condition}')""".format(
                                            _policyarn=policy['Arn'],
                                            _versionid=version['VersionId'],
                                            _actiontype='Action',
                                            _action_raw=action_raw,
                                            _service=action_split[0],
                                            _action=action_split[1],
                                            _action_formatted = str(action).replace('*', "%"),
                                            _resourcetype='Resource',
                                            _resource=resource,
                                            _effect=statement['Effect'],
                                            _condition=c)
                                        #insert records
                                        self.execute_sql(sql_insert)
                                    
                                    resource_list = self.make_list(statement.get('NotResource',[]))
                                    resource = "\n".join(resource_list)
                                    
                                    if len(resource_list)>0:   
                                        if 'Condition' in statement:
                                            c = json.dumps(statement['Condition'])
                                        else:
                                            c = ""
                                        sql_insert = """INSERT INTO managed_policy_actions VALUES (null,'{_policyarn}','{_versionid}','{_actiontype}','{_action_raw}','{_service}','{_action}','{_action_formatted}','{_resourcetype}','{_resource}','{_effect}','{_condition}')""".format(
                                            _policyarn=policy['Arn'],
                                            _versionid=version['VersionId'],
                                            _actiontype='Action',
                                            _action_raw=action_raw,
                                            _service=action_split[0],
                                            _action=action_split[1],
                                            _action_formatted = str(action).replace('*', "%"),
                                            _resourcetype='NotResource',
                                            _resource=resource,
                                            _effect=statement['Effect'],
                                            _condition=c)
                                        #insert records
                                        self.execute_sql(sql_insert)
                                        #print(c)
                                except:
                                    print('error: {}'.format(action))
                            for action in self.make_list(statement.get('NotAction',[])):
                                try:
                                    action_raw=action
                                    action_split = str(action_raw).split(':')
                                    if len(action_split) == 1:
                                        action_split.append('*')     
                                        
                                    resource_list = self.make_list(statement.get('Resource',[]))
                                    resource = "\n".join(resource_list)
                                    
                                    if len(resource_list)>0:                                    
                                        if 'Condition' in statement:
                                            c = json.dumps(statement['Condition'])
                                        else:
                                            c = ""
                                        sql_insert = """INSERT INTO managed_policy_actions VALUES (null,'{_policyarn}','{_versionid}','{_actiontype}','{_action_raw}','{_service}','{_action}','{_action_formatted}','{_resourcetype}','{_resource}','{_effect}','{_condition}')""".format(
                                            _policyarn=policy['Arn'],
                                            _versionid=version['VersionId'],
                                            _actiontype='NotAction',
                                            _action_raw=action,
                                            _service=action_split[0],
                                            _action=action_split[1],
                                            _action_formatted = str(action).replace('*', "%"),
                                            _resourcetype='Resource',
                                            _resource=resource,
                                            _effect=statement['Effect'],
                                            _condition=c)
                                        #insert records
                                        self.execute_sql(sql_insert)

                                    resource_list = self.make_list(statement.get('NotResource',[]))
                                    resource = "\n".join(resource_list)
                                    
                                    if len(resource_list)>0:
                                        if 'Condition' in statement:
                                            c = json.dumps(statement['Condition'])
                                        else:
                                            c = ""
                                        sql_insert = """INSERT INTO managed_policy_actions VALUES (null,'{_policyarn}','{_versionid}','{_actiontype}','{_action_raw}','{_service}','{_action}','{_action_formatted}','{_resourcetype}','{_resource}','{_effect}','{_condition}')""".format(
                                            _policyarn=policy['Arn'],
                                            _versionid=version['VersionId'],
                                            _actiontype='NotAction',
                                            _action_raw=action,
                                            _service=action_split[0],
                                            _action=action_split[1],
                                            _action_formatted = str(action).replace('*', "%"),
                                            _resourcetype='NotResource',
                                            _resource=resource,
                                            _effect=statement['Effect'],
                                            _condition=c)
                                        #insert records
                                        self.execute_sql(sql_insert)
                                        self.execute_sql('CREATE INDEX index_action_formatted ON managed_policy_actions(action_formatted)')
                                        #print(c)
                                except:
                                    print('error: {}'.format(action))
    def __load_user_inline_policy_actions(self):
        print('__load_user_inline_policy_actions')
        for i, user in enumerate(self.raw_data['UserDetailList']):
            if 'UserPolicyList' in user.keys():
                for policy in user['UserPolicyList']:
                    for statement_list in self.make_list(policy['PolicyDocument']):
                        for statement in self.make_list(statement_list['Statement']):
                            for action in self.make_list(statement.get('Action',[])):
                                try:
                                    action_raw=action
                                    action_split = str(action_raw).split(':')
                                    if len(action_split) == 1:
                                        action_split.append('*')     
                                    resource_list = self.make_list(statement.get('Resource',[]))
                                    resource = "\n".join(resource_list)
                                    
                                    if len(resource_list)>0:
                                        if 'Condition' in statement:
                                            c = json.dumps(statement['Condition'])
                                        else:
                                            c = ""
                                        sql_insert = """INSERT INTO users_inline_policies VALUES (null,'{_userarn}','{_policyname}','{_actiontype}','{_action_raw}','{_service}','{_action}','{_action_formatted}','{_resourcetype}','{_resource}','{_effect}','{_condition}')""".format(
                                            _userarn=user['Arn'],
                                            _policyname=policy['PolicyName'],
                                            _actiontype='Action',
                                            _action_raw=action,
                                            _service=action_split[0],
                                            _action=action_split[1],
                                            _action_formatted = str(action).replace('*', "%"),
                                            _resourcetype='Resource',
                                            _resource=resource,
                                            _effect=statement['Effect'],
                                            _condition=c)
                                        #insert records
                                        self.execute_sql(sql_insert)
                                        
                                    resource_list = self.make_list(statement.get('NotResource',[]))
                                    resource = "\n".join(resource_list)
                                    
                                    if len(resource_list)>0:
                                        if 'Condition' in statement:
                                            c = json.dumps(statement['Condition'])
                                        else:
                                            c = ""
                                        sql_insert = """INSERT INTO users_inline_policies VALUES (null,'{_userarn}','{_policyname}','{_actiontype}','{_action_raw}','{_service}','{_action}','{_action_formatted}','{_resourcetype}','{_resource}','{_effect}','{_condition}')""".format(
                                            _userarn=user['Arn'],
                                            _policyname=policy['PolicyName'],
                                            _actiontype='Action',
                                            _action_raw=action,
                                            _service=action_split[0],
                                            _action=action_split[1],
                                            _action_formatted = str(action).replace('*', "%"),
                                            _resourcetype='NotResource',
                                            _resource=resource,
                                            _effect=statement['Effect'],
                                            _condition=c)
                                        #insert records
                                        self.execute_sql(sql_insert)
                                        #print(c)
                                except:
                                    print('error: {}'.format(action))
                            for action in self.make_list(statement.get('NotAction',[])):
                                try:
                                    action_raw=action
                                    action_split = str(action_raw).split(':')
                                    if len(action_split) == 1:
                                        action_split.append('*')                                     
                                    resource_list = self.make_list(statement.get('Resource',[]))
                                    resource = "\n".join(resource_list)
                                    
                                    if len(resource_list)>0:
                                        if 'Condition' in statement:
                                            c = json.dumps(statement['Condition'])
                                        else:
                                            c = ""
                                        sql_insert = """INSERT INTO users_inline_policies VALUES (null,'{_userarn}','{_policyname}','{_actiontype}','{_action_raw}','{_service}','{_action}','{_action_formatted}','{_resourcetype}','{_resource}','{_effect}','{_condition}')""".format(
                                            _userarn=user['Arn'],
                                            _policyname=policy['PolicyName'],
                                            _actiontype='NotAction',
                                            _action_raw=action,
                                            _service=action_split[0],
                                            _action=action_split[1],
                                            _action_formatted = str(action).replace('*', "%"),
                                            _resourcetype='Resource',
                                            _resource=resource,
                                            _effect=statement['Effect'],
                                            _condition=c)
                                        #insert records
                                        self.execute_sql(sql_insert)
                                        
                                    resource_list = self.make_list(statement.get('NotResource',[]))
                                    resource = "\n".join(resource_list)
                                    
                                    if len(resource_list)>0:
                                        if 'Condition' in statement:
                                            c = json.dumps(statement['Condition'])
                                        else:
                                            c = ""
                                        sql_insert = """INSERT INTO users_inline_policies VALUES (null,'{_userarn}','{_policyname}','{_actiontype}','{_action_raw}','{_service}','{_action}','{_action_formatted}','{_resourcetype}','{_resource}','{_effect}','{_condition}')""".format(
                                            _userarn=user['Arn'],
                                            _policyname=policy['PolicyName'],
                                            _actiontype='NotAction',
                                            _action_raw=action,
                                            _service=action_split[0],
                                            _action=action_split[1],
                                            _action_formatted = str(action).replace('*', "%"),
                                            _resourcetype='NotResource',
                                            _resource=resource,
                                            _effect=statement['Effect'],
                                            _condition=c)
                                        #insert records
                                        self.execute_sql(sql_insert)
                                        #print(c)
                                except:
                                    print('error: {}'.format(action))
    def __load_role_assumerolepolicydocument(self):
        print('__load_role_assumerolepolicydocument')
        for i, role in enumerate(self.raw_data['RoleDetailList']):
            for statement_list in self.make_list(role['AssumeRolePolicyDocument']):
                for statement in self.make_list(statement_list['Statement']):
                    for action in self.make_list(statement.get('Action',[])):
                        try:
                            action_raw=action
                            action_split = str(action_raw).split(':')
                            if len(action_split) == 1:
                                action_split.append('*') 
                                
                            if 'Condition' in statement:
                                c = json.dumps(statement['Condition'])
                            else:
                                c = ""
                                
                            principal_keys = ['AWS','Service','CanonicalUser','Federated']
                            principals = []
                            for p in principal_keys:
                                entities = self.make_list(statement['Principal'].get(p,[]))
                                for entity in entities:
                                    principals.append({'type':p, 'entity':entity})
                                  
                            for principal in principals:
                                sql_insert = """INSERT INTO assume_role_policy_documents VALUES (null,"{_rolearn}","{_actiontype}","{_action_raw}","{_action_formatted}","{_service}","{_action}","{_principal_type}","{_principal_entity}","{_effect}",'{_condition}')""".format(
                                    _rolearn=role['Arn'],
                                    _actiontype='Action',
                                    _action_raw=action_raw,
                                    _service=action_split[0],
                                    _action=action_split[1],
                                    _action_formatted = str(action_raw).replace('*', "%"),
                                    _principal_type=principal['type'],
                                    _principal_entity=principal['entity'],
                                    _effect=statement['Effect'],
                                    _condition=c
                                    )
                                #insert records
                                self.execute_sql(sql_insert)
                                
                        except:
                            print('error: {}'.format(action_raw))
    
    def __load_group_inline_policy_actions(self):
        print('__load_group_inline_policy_actions')
        for i, group in enumerate(self.raw_data['GroupDetailList']):
            if 'GroupPolicyList' in group.keys():
                for policy in group['GroupPolicyList']:
                    for statement_list in self.make_list(policy['PolicyDocument']):
                        for statement in self.make_list(statement_list['Statement']):
                            for action in self.make_list(statement.get('Action',[])):
                                try:
                                    action_raw=action
                                    action_split = str(action_raw).split(':')
                                    if len(action_split) == 1:
                                        action_split.append('*')     
                                    
                                    resource_list = self.make_list(statement.get('Resource',[]))
                                    resource = "\n".join(resource_list)
                                    
                                    if len(resource_list)>0:
                                        if 'Condition' in statement:
                                            c = json.dumps(statement['Condition'])
                                        else:
                                            c = ""
                                        sql_insert = """INSERT INTO groups_inline_policies VALUES (null,'{_groupname}','{_grouparn}','{_policyname}','{_actiontype}','{_action_raw}','{_service}','{_action}','{_action_formatted}','{_resourcetype}','{_resource}','{_effect}','{_condition}')""".format(
                                            _groupname=group['GroupName'],
                                            _grouparn=group['Arn'],
                                            _policyname=policy['PolicyName'],
                                            _actiontype='Action',
                                            _action_raw=action_raw,
                                            _service=action_split[0],
                                            _action=action_split[1],
                                            _action_formatted = str(action).replace('*', "%"),
                                            _resourcetype='Resource',
                                            _resource=resource,
                                            _effect=statement['Effect'],
                                            _condition=c)
                                        #insert records
                                        self.execute_sql(sql_insert)
                                    
                                    resource_list = self.make_list(statement.get('NotResource',[]))
                                    resource = "\n".join(resource_list)
                                    
                                    if len(resource_list)>0:
                                        if 'Condition' in statement:
                                            c = json.dumps(statement['Condition'])
                                        else:
                                            c = ""
                                        sql_insert = """INSERT INTO groups_inline_policies VALUES (null,'{_groupname}','{_grouparn}','{_policyname}','{_actiontype}','{_action_raw}','{_service}','{_action}','{_action_formatted}','{_resourcetype}','{_resource}','{_effect}','{_condition}')""".format(
                                            _groupname=group['GroupName'],
                                            _grouparn=group['Arn'],
                                            _policyname=policy['PolicyName'],
                                            _actiontype='Action',
                                            _action_raw=action_raw,
                                            _service=action_split[0],
                                            _action=action_split[1],
                                            _action_formatted = str(action).replace('*', "%"),
                                            _resourcetype='NotResource',
                                            _resource=resource,
                                            _effect=statement['Effect'],
                                            _condition=c)
                                        #insert records
                                        self.execute_sql(sql_insert)
                                        #print(c)
                                except:
                                    print('error: {}'.format(action))
                            for action in self.make_list(statement.get('NotAction',[])):
                                try:
                                    action_raw=action
                                    action_split = str(action_raw).split(':')
                                    if len(action_split) == 1:
                                        action_split.append('*')      
                                        
                                    resource_list = self.make_list(statement.get('Resource',[]))
                                    resource = "\n".join(resource_list)
                                    
                                    if len(resource_list)>0:
                                        if 'Condition' in statement:
                                            c = json.dumps(statement['Condition'])
                                        else:
                                            c = ""
                                        sql_insert = """INSERT INTO groups_inline_policies VALUES (null,'{_groupname}','{_grouparn}','{_policyname}','{_actiontype}','{_action_raw}','{_service}','{_action}','{_action_formatted}','{_resourcetype}','{_resource}','{_effect}','{_condition}')""".format(
                                            _groupname=group['GroupName'],
                                            _grouparn=group['Arn'],
                                            _policyname=policy['PolicyName'],
                                            _actiontype='NotAction',
                                            _action_raw=action_raw,
                                            _service=action_split[0],
                                            _action=action_split[1],
                                            _action_formatted = str(action).replace('*', "%"),
                                            _resourcetype='Resource',
                                            _resource=resource,
                                            _effect=statement['Effect'],
                                            _condition=c)
                                        #insert records
                                        self.execute_sql(sql_insert)
                                    
                                    resource_list = self.make_list(statement.get('NotResource',[]))
                                    resource = "\n".join(resource_list)
                                    
                                    if len(resource_list)>0:
                                        if 'Condition' in statement:
                                            c = json.dumps(statement['Condition'])
                                        else:
                                            c = ""
                                        sql_insert = """INSERT INTO groups_inline_policies VALUES (null,'{_groupname}','{_grouparn}','{_policyname}','{_actiontype}','{_action_raw}','{_service}','{_action}','{_action_formatted}','{_resourcetype}','{_resource}','{_effect}','{_condition}')""".format(
                                            _groupname=group['GroupName'],
                                            _grouparn=group['Arn'],
                                            _policyname=policy['PolicyName'],
                                            _actiontype='NotAction',
                                            _action_raw=action_raw,
                                            _service=action_split[0],
                                            _action=action_split[1],
                                            _action_formatted = str(action).replace('*', "%"),
                                            _resourcetype='NotResource',
                                            _resource=resource,
                                            _effect=statement['Effect'],
                                            _condition=c)
                                        #insert records
                                        self.execute_sql(sql_insert)
                                        #print(c)
                                except:
                                    print('error: {}'.format(action))
                                    
    def __load_role_inline_policy_actions(self):
        print('__load_role_inline_policy_actions')
        for i, role in enumerate(self.raw_data['RoleDetailList']):
            if 'RolePolicyList' in role.keys():
                for policy in role['RolePolicyList']:
                    for statement_list in self.make_list(policy['PolicyDocument']):
                        for statement in self.make_list(statement_list['Statement']):
                            for action in self.make_list(statement.get('Action',[])):
                                try:
                                    action_raw=action
                                    action_split = str(action_raw).split(':')
                                    if len(action_split) == 1:
                                        action_split.append('*')  
                                    
                                    resource_list = self.make_list(statement.get('Resource',[]))
                                    resource = "\n".join(resource_list)
                                    
                                    if len(resource_list)>0:
                                        if 'Condition' in statement:
                                            c = json.dumps(statement['Condition'])
                                        else:
                                            c = ""
                                        sql_insert = """INSERT INTO roles_inline_policies VALUES (null,"{_rolearn}","{_policyname}","{_actiontype}","{_action_raw}","{_service}","{_action}","{_action_formatted}","{_resourcetype}","{_resource}","{_effect}",'{_condition}')""".format(
                                            _rolearn=role['Arn'],
                                            _policyname=policy['PolicyName'],
                                            _actiontype='Action',
                                            _action_raw=action_raw,
                                            _service=action_split[0],
                                            _action=action_split[1],
                                            _action_formatted = str(action).replace('*', "%"),
                                            _resourcetype='Resource',
                                            _resource=resource,
                                            _effect=statement['Effect'],
                                            _condition=c)
                                        #insert records
                                        self.execute_sql(sql_insert)
                                    
                                    resource_list = self.make_list(statement.get('NotResource',[]))
                                    resource = "\n".join(resource_list)
                                    
                                    if len(resource_list)>0:
                                        if 'Condition' in statement:
                                            c = json.dumps(statement['Condition'])
                                        else:
                                            c = ""
                                        sql_insert = """INSERT INTO roles_inline_policies VALUES (null,"{_rolearn}","{_policyname}","{_actiontype}","{_action_raw}","{_service}","{_action}","{_action_formatted}","{_resourcetype}","{_resource}","{_effect}",'{_condition}')""".format(
                                            _rolearn=role['Arn'],
                                            _policyname=policy['PolicyName'],
                                            _actiontype='Action',
                                            _action_raw=action_raw,
                                            _service=action_split[0],
                                            _action=action_split[1],
                                            _action_formatted = str(action).replace('*', "%"),
                                            _resourcetype='NotResource',
                                            _resource=resource,
                                            _effect=statement['Effect'],
                                            _condition=c)
                                        #insert records
                                        self.execute_sql(sql_insert)
                                        #print(c)
                                except:
                                    print('error: {}'.format(action))
                            for action in self.make_list(statement.get('NotAction',[])):
                                try:
                                    action_raw=action
                                    action_split = str(action_raw).split(':')
                                    if len(action_split) == 1:
                                        action_split.append('*')                                  
                                    
                                    resource_list = self.make_list(statement.get('Resource',[]))
                                    resource = "\n".join(resource_list)
                                    
                                    if len(resource_list)>0:
                                        if 'Condition' in statement:
                                            c = json.dumps(statement['Condition'])
                                        else:
                                            c = ""
                                        sql_insert = """INSERT INTO roles_inline_policies VALUES (null,"{_rolearn}","{_policyname}","{_actiontype}","{_action_raw}","{_service}","{_action}","{_action_formatted}","{_resourcetype}","{_resource}","{_effect}",'{_condition}')""".format(
                                            _rolearn=role['Arn'],
                                            _policyname=policy['PolicyName'],
                                            _actiontype='NotAction',
                                            _action_raw=action_raw,
                                            _service=action_split[0],
                                            _action=action_split[1],
                                            _action_formatted = str(action).replace('*', "%"),
                                            _resourcetype='Resource',
                                            _resource=resource,
                                            _effect=statement['Effect'],
                                            _condition=c)
                                        #insert records
                                        self.execute_sql(sql_insert)
                                    
                                    resource_list = self.make_list(statement.get('NotResource',[]))
                                    resource = "\n".join(resource_list)
                                    
                                    if len(resource_list)>0:
                                        if 'Condition' in statement:
                                            c = json.dumps(statement['Condition'])
                                        else:
                                            c = ""
                                        sql_insert = """INSERT INTO roles_inline_policies VALUES (null,"{_rolearn}","{_policyname}","{_actiontype}","{_action_raw}","{_service}","{_action}","{_action_formatted}","{_resourcetype}","{_resource}","{_effect}",'{_condition}')""".format(
                                            _rolearn=role['Arn'],
                                            _policyname=policy['PolicyName'],
                                            _actiontype='NotAction',
                                            _action_raw=action_raw,
                                            _service=action_split[0],
                                            _action=action_split[1],
                                            _action_formatted = str(action).replace('*', "%"),
                                            _resourcetype='NotResource',
                                            _resource=resource,
                                            _effect=statement['Effect'],
                                            _condition=c)
                                        #insert records
                                        self.execute_sql(sql_insert)
                                        #print(c)
                                except:
                                    print('error: {}'.format(action))
                                    
    @property
    def group_detail_list(self): return self.raw_data['GroupDetailList']
    
    @property
    def user_detail_list(self): return self.raw_data['UserDetailList']
    
    @property
    def group_detail_list(self): return self.raw_data['RoleDetailList']
    
    @property
    def policies(self): return self.raw_data['Policies']
    
    @property
    def users(self):
        q = """SELECT * FROM users;"""
        return pd.read_sql_query(q,self.conn)
    
    @property
    def services(self):
        q = """SELECT DISTINCT service FROM actions_table;"""
        return pd.read_sql_query(q,self.conn)
    
    @property
    def access_levels(self):
        q = """SELECT DISTINCT access_level FROM actions_table;"""
        return pd.read_sql_query(q,self.conn)
    
    @property
    def actions(self):
        q = """SELECT DISTINCT service, action, description, access_level FROM actions_table;"""
        df = pd.read_sql_query(q,self.conn)
        df['formatted'] = df['service'] + ":" + df['action']
        return df

'''
if __name__ == '__main__':

    db = LpMetricsDb('lpmetrics.db')  
