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
#import xlwings as xw
import sqlite3
from sqlite3 import Error
#from datetime import datetime
#from pypika import Query, Table, Field
from webscrape import AwsActionTable

class Database:
    def __init__(self,_db_identifier=None):
        self.conn = None
        
        if _db_identifier is None:
            self.db_identifier = ":memory:"
        else:
            self.db_identifier = _db_identifier
            
        self.conn = self.create_connection(self.db_identifier)
        
    def create_connection(self,name):
        conn = None
        try:
            conn = sqlite3.connect(name)
        except Error as e:
            print(e)
        return conn
    
    def execute_sql(self,_sql_statement):
        if self.conn is not None:
            try:
                c = self.conn.cursor()
                c.execute(_sql_statement)
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

class IamConfig(Database):
    
    def __init__(self,_json_file=None,_data=None,_db_identifier=None):
        Database.__init__(self, _db_identifier)
        self.raw_data = None
        self.create_table_statements = self.get_create_table_statements()
        
        self.awsAT = AwsActionTable()
        #self.awsAT.load_csv('actions_table.csv')
        self.awsAT.full_table.to_sql('actions_table',self.conn,index=False)
        
        if _data is not None and _json_file is None:
            self.load_data(_data)
        elif _data is None and _json_file is not None:
            self.load_json_data_from_file(_json_file)
        else:
            print("Must initiate the instance with either json file or data parameters, not both")

 
    def load_json_data_from_file(self,filename):
        with open(filename) as f:
            _data = json.load(f)
            self.raw_data = _data
        self.__create_tables()
        self.__load_data_to_db()        
    
    def load_data(self,_data):
        self.raw_data = _data
        self.__create_tables()
        self.__load_data_to_db()
        
    def __create_tables(self):
        for query in self.create_table_statements.items():
            self.execute_sql(query[1])
            
    @staticmethod
    def get_create_table_statements():
        statements = {}
        
        #users table
        __sql_create_users_table = """CREATE TABLE IF NOT EXISTS users (
                                        arn text PRIMARY KEY,
                                        username text NOT NULL,
                                        userid text NOT NULL,
                                        createdate text NOT NULL
                                    );"""
        statements['users'] = __sql_create_users_table

        #user_group_membership table
        __sql_create_user_to_group_table = """CREATE TABLE IF NOT EXISTS user_group_membership (
                                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                                        userarn text NOT NULL,
                                        groupname text NOT NULL
                                    );"""
        statements['user_group_membership'] = __sql_create_user_to_group_table

        #users_managed_policies table
        __sql_create_user_to_managed_policy_table = """CREATE TABLE IF NOT EXISTS users_managed_policies (
                                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                                        userarn text NOT NULL,
                                        policyarn text NOT NULL
                                    );"""
        statements['users_managed_policies'] = __sql_create_user_to_managed_policy_table
        
        #roles_managed_policies table
        __sql_create_role_to_managed_policy_table = """CREATE TABLE IF NOT EXISTS roles_managed_policies (
                                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                                        rolearn text NOT NULL,
                                        policyarn text NOT NULL
                                    );"""
        statements['roles_managed_policies'] = __sql_create_role_to_managed_policy_table
        
        #groups table
        __sql_create_groups_table = """CREATE TABLE IF NOT EXISTS groups (
                                        arn text PRIMARY KEY,
                                        groupname text NOT NULL,
                                        groupid text NOT NULL,
                                        createdate text NOT NULL
                                    );"""
        statements['groups'] = __sql_create_groups_table

        #groups_managed_policies table
        __sql_create_groups_to_managed_policy_table = """CREATE TABLE IF NOT EXISTS groups_managed_policies (
                                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                                        grouparn text NOT NULL,
                                        groupname text NOT NULL,
                                        policyarn text NOT NULL
                                    );"""
        statements['groups_managed_policies'] = __sql_create_groups_to_managed_policy_table

        #managed_policies table
        __sql_create_managed_policy_table = """CREATE TABLE IF NOT EXISTS managed_policies (
                                        arn text PRIMARY KEY,
                                        attachmentcount integer NOT NULL,
                                        createdate text NOT NULL,
                                        defaultversionid text NOT NULL,
                                        isattachable boolean NOT NULL,
                                        policyid text NOT NULL,
                                        policyname text NOT NULL,
                                        updatedate text NOT NULL
                                    );"""
        statements['managed_policies'] = __sql_create_managed_policy_table
        
        #users_inline_policies_actions
        __sql_create_users_inline_policy_actions_table = """CREATE TABLE IF NOT EXISTS users_inline_policies (
                                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                                        userarn text NOT NULL,
                                        policyname text NOT NULL,
                                        actiontype text NOT NULL,
                                        action_raw text NOT NULL,
                                        action_service text,
                                        action text,
                                        action_formatted text NOT NULL,
                                        resourcetype text NOT NULL,
                                        resource text NOT NULL,
                                        effect text NOT NULL,
                                        condition json
                                    );"""
        statements['user_inline_policy_actions'] = __sql_create_users_inline_policy_actions_table
        
        #groups_inline_policies_actions
        __sql_create_groups_inline_policy_actions_table = """CREATE TABLE IF NOT EXISTS groups_inline_policies (
                                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                                        groupname text NOT NULL,
                                        grouparn text NOT NULL,
                                        policyname text NOT NULL,
                                        actiontype text NOT NULL,
                                        action_raw text NOT NULL,
                                        action_service text,
                                        action text,
                                        action_formatted text NOT NULL,
                                        resourcetype text NOT NULL,
                                        resource text NOT NULL,
                                        effect text NOT NULL,
                                        condition json
                                    );"""
        statements['group_inline_policy_actions'] = __sql_create_groups_inline_policy_actions_table

        #roles_inline_policies_actions
        __sql_create_role_inline_policy_actions_table = """CREATE TABLE IF NOT EXISTS roles_inline_policies (
                                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                                        rolearn text NOT NULL,
                                        policyname text NOT NULL,
                                        actiontype text NOT NULL,
                                        action_raw text NOT NULL,
                                        action_service text,
                                        action text,
                                        action_formatted text NOT NULL,
                                        resourcetype text NOT NULL,
                                        resource text NOT NULL,
                                        effect text NOT NULL,
                                        condition json
                                    );"""
        statements['role_inline_policy_actions'] = __sql_create_role_inline_policy_actions_table
        
        #managed_policy_actions table
        __sql_create_managed_policy_actions_table = """CREATE TABLE IF NOT EXISTS managed_policy_actions (
                                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                                        policyarn text NOT NULL,
                                        versionid text NOT NULL,
                                        actiontype text NOT NULL,
                                        action_raw text NOT NULL,
                                        action_service text,
                                        action text,
                                        action_formatted text NOT NULL,
                                        resourcetype text NOT NULL,
                                        resource text NOT NULL,
                                        effect text NOT NULL,
                                        condition json
                                    );"""
        statements['managed_policy_actions'] = __sql_create_managed_policy_actions_table

        #roles table
        __sql_create_roles_table = """CREATE TABLE IF NOT EXISTS roles (
                                        arn text PRIMARY KEY,
                                        rolename text NOT NULL,
                                        roleid text NOT NULL,
                                        createdate text NOT NULL
                                    );"""
        statements['roles'] = __sql_create_roles_table
        
        #assumeRolePolicyDocuments table
        __sql_create_arpd_table = """CREATE TABLE IF NOT EXISTS assume_role_policy_documents (
                                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                                        rolearn text NOT NULL,
                                        actiontype text NOT NULL,
                                        action_raw text NOT NULL,
                                        action_service text,
                                        action text,
                                        action_formatted text NOT NULL,
                                        principal_type text NOT NULL,
                                        principal_entity text NOT NULL,
                                        effect text NOT NULL,
                                        condition json
                                    );"""
        statements['arpd'] = __sql_create_arpd_table
        
        
        
        return statements
    
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


if __name__ == '__main__':

    iam = IamConfig(_json_file='CY_auth.json')