#!/usr/bin/env python
# coding: utf-8
'''
need to add resource_type to run query function
need to add role permissions by role inline policies
modify rest of sheets to be created before 'zzz'

'''

import tkinter as tk
#from tkinter import Tk
#from tkinter import ttk
from tkinter import filedialog
import sqlite3
from sqlite3 import Error
import json
import pandas as pd
import xlwings as xw
import requests
from bs4 import BeautifulSoup

from aws_iam_backup import IamConfig
from webscrape import AwsActionTable

__description__ = "Search access within IAM"


class App(tk.Tk):
    
    def __init__(self, debug=False):
        super(App, self).__init__()
        #self.filename = ""
        self.configure_gui()
        self.create_widgets()
        self.wb = None
        self.iam = None
        self.__query_ready = False
        self.results_query_sht = None
        self.results_services_sht = None
        self.results_users_sht = None
        self.role_permissions_sht = None
        self.debug = debug

    def configure_gui(self):
        width=150
        height=500
        screenwidth = self.winfo_screenwidth()
        screenheight = self.winfo_screenheight()
        alignstr = '%dx%d+%d+%d' % (width, height, (screenwidth - width) / 2, (screenheight - height) / 2)
        self.geometry(alignstr)

        self.title("AWS Analyze IAM")
        #self.geometry("500x500")
        self.resizable(False,False)
        #self.configure(bg="")

        #bgc=self.cget('bg')
        #bgc="red"
        #rel = tk.FLAT


    def create_widgets(self):
        self.label = tk.Label(text="Load Data")
        self.label.place(x=10,y=60,width=109,height=30)

        self.btn_open = tk.Button(self,text="Open", command=self.open_file)
        self.btn_open.place(x=10,y=20,width=109,height=30)
        
        self.btn_query_builder = tk.Button(self,text="Query Builder", command=self.build_query_command)
        self.btn_query_builder.place(x=10,y=100,width=109,height=30)
        
        self.btn_run_query = tk.Button(self,text="Run Query", command=self.run_query_command)
        self.btn_run_query.place(x=10,y=140,width=109,height=30)
        
        self.btn_get_users = tk.Button(self,text="Get List of Users", command=self.run_get_users_command)
        self.btn_get_users.place(x=10,y=180,width=109,height=30)
        
        self.btn_get_services = tk.Button(self,text="Get List of Services", command=self.run_get_services_command)
        self.btn_get_services.place(x=10,y=220,width=109,height=30)
        
    def test(self):
        print(self.chk_inclRes_val.get())

    def open_file(self):
        if self.debug:
            fname= "C:/Users/jkuhnsman/Desktop/CY_auth.json"
        else:
            fname = filedialog.askopenfilename(initialdir = "/",title = "Select file",filetypes=[("JSON", ".json")])
        
        if fname == '':
            print('do nothing')
        else:
            with open(fname) as f:
                #self.raw_data = json.load(f)
                self.label['text'] = 'Data Loaded!'
                self.iam = IamConfig(_json_file=fname)
                print(fname)
    
    def build_query_command(self):
        if self.iam is not None:
            self.generate_query_builder()
            self.__query_ready = True
    
    def run_get_users_command(self):
        if self.iam is not None:
            
            if self.wb == None: self.generate_workbook()
                
            if self.results_users_sht is None:
                self.results_users_sht = self.wb.sheets.add('Results_Users', before='zzz')
            else:
                self.results_users_sht.clear()
            
            
            self.run_query_users(self.results_users_sht)
            
    def run_get_services_command(self):
        if self.iam is not None:
            
            if self.wb == None: self.generate_workbook()
            
            if self.results_services_sht is None:
                self.results_services_sht = self.wb.sheets.add('Results_Services', before='zzz')
            else:
                self.results_services_sht.clear()          
            
            self.run_query_services(self.results_services_sht)
            
    def run_query_command(self):
        if self.__query_ready:
            if self.results_query_sht is None:
                self.results_query_sht = self.wb.sheets.add('Results_Users_Query')
            else:
                self.results_query_sht.clear()
      
            if self.role_permissions_sht is None:
                self.role_permissions_sht = self.wb.sheets.add('Results_Roles_Query')
            else:
                self.role_permissions_sht.clear()
            
            
            self.run_query_roles(self.role_permissions_sht)
            self.run_query_query(self.results_query_sht)
    
    def run_query_roles(self,sheet):
        df = self.wb.sheets['Services'].range('A1').expand().options(pd.DataFrame,index=False).value
        service_list = df['service'][df.Included=='Yes'].to_list()
        print(service_list)

        df = self.wb.sheets['Access Levels'].range('A1').expand().options(pd.DataFrame,index=False).value
        accesslvl_list = df['access_level'][df.Included=='Yes'].to_list()

        df = self.wb.sheets['Actions'].range('A1').expand().options(pd.DataFrame,index=False).value
        actions_list = df['formatted'][df.Included=='Yes'].to_list()
        
        #query to get users with privileges granted by role managed policies
        sql = """SELECT DISTINCT
        assume_role_policy_documents.principal_entity,
        assume_role_policy_documents.principal_type,
        assume_role_policy_documents.condition AS principal_condition,
        roles_managed_policies.rolearn,
        roles_managed_policies.policyarn,
        'AttachedManagedPolicy' AS policy_type,
        managed_policy_actions.effect,
        managed_policy_actions.action_raw,
        managed_policy_actions.resource,
        managed_policy_actions.actiontype,
        IIF(managed_policy_actions.action_raw = '*', 'All Levels',
            IIF(managed_policy_actions.action_raw LIKE '%:*', 'All Levels', actions_table.access_level)) AS Access_Level,
        managed_policy_actions.condition,
        IIF(managed_policy_actions.action_raw = '*', 'All Services', actions_table.service) AS Service,
        IIF(managed_policy_actions.action_raw = '*', 'All Actions',
            IIF(managed_policy_actions.action_raw LIKE '%:*', 'All Actions', actions_table.action)) AS Action
        
        FROM assume_role_policy_documents
        
        JOIN roles_managed_policies ON assume_role_policy_documents.rolearn=roles_managed_policies.rolearn
        LEFT JOIN managed_policies ON managed_policies.arn=roles_managed_policies.policyarn
        LEFT JOIN managed_policy_actions ON managed_policy_actions.policyarn=managed_policies.arn
        LEFT JOIN actions_table ON actions_table.service_action_combined LIKE managed_policy_actions.action_formatted
        """
        
        if len(service_list) > 0 or len(actions_list) > 0 or len(accesslvl_list) > 0:
            sql += "\nWHERE"
            sql += "\n("
            if len(service_list) > 0:
                sql += "\nmanaged_policy_actions.action_raw LIKE '*' \nOR\n"
                for i, service in enumerate(service_list):
                    sql += "managed_policy_actions.action_formatted LIKE \"{}:%\"".format(service)
                    if i < (len(service_list)-1):
                        sql += "\nOR\n"
                    else:
                        sql += "\n"
                        
            if len(actions_list) > 0:
                if len(service_list) > 0: sql += "OR\n"
                for i, action in enumerate(actions_list):
                    sql += "actions_table.service_action_combined LIKE \"{}\"".format(action)
                    if i < (len(actions_list)-1):
                        sql += "\nOR\n"
                    else:
                        sql += "\n"
            sql += ")"
            
            if len(accesslvl_list) > 0:
                if len(service_list) > 0 or len(actions_list) > 0: sql += "\nAND"
                sql += "\n("
                for i, action in enumerate(accesslvl_list):
                    sql += " actions_table.access_level LIKE \"{}\"".format(action)
                    if i < (len(accesslvl_list)-1):
                        sql += " OR"
                    else:
                        sql += ")"
        print(sql)
        print('\nQuery is running')
        
        df_roles = pd.read_sql_query(sql,self.iam.conn)
        print('\nQuery complete')
        
        
        
        #query to get users with privileges granted by role inline policies
        sql = """SELECT DISTINCT
        assume_role_policy_documents.principal_entity,
        assume_role_policy_documents.principal_type,
        assume_role_policy_documents.condition AS principal_condition,
        roles_inline_policies.rolearn,
        'Inline' AS policy_type,
        roles_inline_policies.effect,
        roles_inline_policies.action_raw,
        roles_inline_policies.resource,
        roles_inline_policies.actiontype,
        IIF(roles_inline_policies.action_raw = '*', 'All Levels',
            IIF(roles_inline_policies.action_raw LIKE '%:*', 'All Levels', actions_table.access_level)) AS Access_Level,
        roles_inline_policies.condition,
        IIF(roles_inline_policies.action_raw = '*', 'All Services', actions_table.service) AS Service,
        IIF(roles_inline_policies.action_raw = '*', 'All Actions',
            IIF(roles_inline_policies.action_raw LIKE '%:*', 'All Actions', actions_table.action)) AS Action
        
        FROM assume_role_policy_documents
        
        JOIN roles_inline_policies ON assume_role_policy_documents.rolearn=roles_inline_policies.rolearn
        LEFT JOIN actions_table ON actions_table.service_action_combined LIKE roles_inline_policies.action_formatted
        """
        
        if len(service_list) > 0 or len(actions_list) > 0 or len(accesslvl_list) > 0:
            sql += "\nWHERE"
            sql += "\n("
            if len(service_list) > 0:
                sql += "\nroles_inline_policies.action_raw LIKE '*' \nOR\n"
                for i, service in enumerate(service_list):
                    sql += "roles_inline_policies.action_formatted LIKE \"{}:%\"".format(service)
                    if i < (len(service_list)-1):
                        sql += "\nOR\n"
                    else:
                        sql += "\n"
                        
            if len(actions_list) > 0:
                if len(service_list) > 0: sql += "OR\n"
                for i, action in enumerate(actions_list):
                    sql += "actions_table.service_action_combined LIKE \"{}\"".format(action)
                    if i < (len(actions_list)-1):
                        sql += "\nOR\n"
                    else:
                        sql += "\n"
            sql += ")"
            
            if len(accesslvl_list) > 0:
                if len(service_list) > 0 or len(actions_list) > 0: sql += "\nAND"
                sql += "\n("
                for i, action in enumerate(accesslvl_list):
                    sql += " actions_table.access_level LIKE \"{}\"".format(action)
                    if i < (len(accesslvl_list)-1):
                        sql += " OR"
                    else:
                        sql += ")"
        print(sql)
        print('\nQuery is running')
        
        df_rolesInline = pd.read_sql_query(sql,self.iam.conn)
        print('\nQuery complete')
        
        
        df_full = pd.concat([df_roles,df_rolesInline])
        sheet.range('A1').options(index=False,header=True).value=df_full
    
    #find all unique users and principals
    def run_query_users(self,sheet):
        
        #query to get users with privileges granted by assuming a role
        sql = """
        SELECT
            username,
            arn,
            createdate
        FROM
            users
        """
        
        print(sql)
        print('\nQuery is running')
        
        df = pd.read_sql_query(sql,self.iam.conn)
        print('\nQuery complete')
        
        sheet.range('A1').options(index=False,header=True).value=df
        
    #find all unique users and principals
    def run_query_services(self,sheet):
        
        #query to get users with privileges granted by assuming a role
        sql = """
        SELECT DISTINCT
            managed_policy_actions.action_service
        
        FROM
            managed_policies
        JOIN managed_policy_actions ON managed_policy_actions.policyarn=managed_policies.arn
        
        WHERE
            managed_policies.attachmentcount > 0
            
        UNION
        
        SELECT DISTINCT
            action_service
        
        FROM
            users_inline_policies
            
        UNION
        
        SELECT DISTINCT
            action_service
        
        FROM
            groups_inline_policies
        
        UNION
        
        SELECT DISTINCT
            action_service
        
        FROM
            roles_inline_policies
        ORDER BY
            action_service
        """
        
        print(sql)
        print('\nQuery is running')
        
        df = pd.read_sql_query(sql,self.iam.conn)
        print('\nQuery complete')
        
        sheet.range('A1').options(index=False,header=True).value=df
        
    def run_query_query(self,sheet):
        df = self.wb.sheets['Services'].range('A1').expand().options(pd.DataFrame,index=False).value
        service_list = df['service'][df.Included=='Yes'].to_list()
        print(service_list)

        df = self.wb.sheets['Access Levels'].range('A1').expand().options(pd.DataFrame,index=False).value
        accesslvl_list = df['access_level'][df.Included=='Yes'].to_list()

        df = self.wb.sheets['Actions'].range('A1').expand().options(pd.DataFrame,index=False).value
        actions_list = df['formatted'][df.Included=='Yes'].to_list()       
        
        #query to get users with privileges assigned by group managed policies
        sql = """SELECT DISTINCT users.username,
        user_group_membership.groupname,
        managed_policies.policyname,
        'AttachedManagedPolicy' AS policy_type,
        managed_policy_actions.effect,
        managed_policy_actions.action_raw,
        managed_policy_actions.resource,
        managed_policy_actions.actiontype,
        IIF(managed_policy_actions.action_raw = '*', 'All Levels',
            IIF(managed_policy_actions.action_raw LIKE '%:*', 'All Levels', actions_table.access_level)) AS Access_Level,
        managed_policy_actions.condition,
        IIF(managed_policy_actions.action_raw = '*', 'All Services', actions_table.service) AS Service,
        IIF(managed_policy_actions.action_raw = '*', 'All Actions',
            IIF(managed_policy_actions.action_raw LIKE '%:*', 'All Actions', actions_table.action)) AS Action
        
        FROM users
        
        JOIN user_group_membership ON users.arn=user_group_membership.userarn
        LEFT JOIN groups ON user_group_membership.groupname=groups.groupname
        LEFT JOIN groups_managed_policies ON groups_managed_policies.grouparn=groups.arn
        LEFT JOIN managed_policies ON managed_policies.arn=groups_managed_policies.policyarn
        LEFT JOIN managed_policy_actions ON managed_policy_actions.policyarn=managed_policies.arn
        LEFT JOIN actions_table ON actions_table.service_action_combined LIKE managed_policy_actions.action_formatted
        """
        
        if len(service_list) > 0 or len(actions_list) > 0 or len(accesslvl_list) > 0:
            sql += "\nWHERE"
            sql += "\n("
            if len(service_list) > 0:
                sql += "\nmanaged_policy_actions.action_raw LIKE '*' \nOR\n"
                for i, service in enumerate(service_list):
                    sql += "managed_policy_actions.action_formatted LIKE \"{}:%\"".format(service)
                    if i < (len(service_list)-1):
                        sql += "\nOR\n"
                    else:
                        sql += "\n"
                        
            if len(actions_list) > 0:
                if len(service_list) > 0: sql += "OR\n"
                for i, action in enumerate(actions_list):
                    sql += "actions_table.service_action_combined LIKE \"{}\"".format(action)
                    if i < (len(actions_list)-1):
                        sql += "\nOR\n"
                    else:
                        sql += "\n"
            sql += ")"
            
            if len(accesslvl_list) > 0:
                if len(service_list) > 0 or len(actions_list) > 0: sql += "\nAND"
                sql += "\n("
                for i, action in enumerate(accesslvl_list):
                    sql += " actions_table.access_level LIKE \"{}\"".format(action)
                    if i < (len(accesslvl_list)-1):
                        sql += " OR"
                    else:
                        sql += ")"
             
        print(sql)
        print('\nQuery is running')
        
        df_usersByGroupAttachedPolicy = pd.read_sql_query(sql,self.iam.conn)
        print('\nQuery complete')
        
        #query to get users with privileges assigned by group inline policies
        sql = """SELECT DISTINCT users.username,
        user_group_membership.groupname,
        groups_inline_policies.policyname,
        'InlinePolicy' AS policy_type,
        groups_inline_policies.effect,
        groups_inline_policies.action_raw,
        groups_inline_policies.resource,
        groups_inline_policies.actiontype,
        IIF(groups_inline_policies.action_raw = '*', 'All Levels',
            IIF(groups_inline_policies.action_raw LIKE '%:*', 'All Levels', actions_table.access_level)) AS Access_Level,
        groups_inline_policies.condition,
        IIF(groups_inline_policies.action_raw = '*', 'All Services', actions_table.service) AS Service,
        IIF(groups_inline_policies.action_raw = '*', 'All Actions',
            IIF(groups_inline_policies.action_raw LIKE '%:*', 'All Actions', actions_table.action)) AS Action
        
        FROM users
        
        JOIN user_group_membership ON users.arn=user_group_membership.userarn
        LEFT JOIN groups ON user_group_membership.groupname=groups.groupname
        LEFT JOIN groups_inline_policies ON groups_inline_policies.grouparn=groups.arn
        LEFT JOIN actions_table ON actions_table.service_action_combined LIKE groups_inline_policies.action_formatted
        """
        
        if len(service_list) > 0 or len(actions_list) > 0 or len(accesslvl_list) > 0:
            sql += "\nWHERE"
            sql += "\n("
            if len(service_list) > 0:
                sql += "\ngroups_inline_policies.action_raw LIKE '*' \nOR\n"
                for i, service in enumerate(service_list):
                    sql += " groups_inline_policies.action_formatted LIKE \"{}:%\"".format(service)
                    if i < (len(service_list)-1):
                        sql += "\nOR\n"
                    else:
                        sql += "\n"
                        
            if len(actions_list) > 0:
                if len(service_list) > 0: sql += "OR\n"
                for i, action in enumerate(actions_list):
                    sql += "actions_table.service_action_combined LIKE \"{}\"".format(action)
                    if i < (len(actions_list)-1):
                        sql += "\nOR\n"
                    else:
                        sql += "\n"
            sql += ")"
            
            if len(accesslvl_list) > 0:
                if len(service_list) > 0 or len(actions_list) > 0: sql += "\nAND"
                sql += "\n("
                for i, action in enumerate(accesslvl_list):
                    sql += " actions_table.access_level LIKE \"{}\"".format(action)
                    if i < (len(accesslvl_list)-1):
                        sql += " OR"
                    else:
                        sql += ")"
        print(sql)
        print('\nQuery is running')
        df_usersByGroupInline = pd.read_sql_query(sql,self.iam.conn)
        print('\nQuery complete')
        
        #query to get permissions through user attached managed policies
        sql = """SELECT DISTINCT users.username,
        '' AS groupname,
        managed_policies.policyname,
        'AttachedManagedPolicy' AS policy_type,
        managed_policy_actions.effect,
        managed_policy_actions.action_raw,
        managed_policy_actions.resource,
        managed_policy_actions.actiontype,
        IIF(managed_policy_actions.action_raw = '*', 'All Levels',
            IIF(managed_policy_actions.action_raw LIKE '%:*', 'All Levels', actions_table.access_level)) AS Access_Level,
        managed_policy_actions.condition,
        IIF(managed_policy_actions.action_raw = '*', 'All Services', actions_table.service) AS Service,
        IIF(managed_policy_actions.action_raw = '*', 'All Actions',
            IIF(managed_policy_actions.action_raw LIKE '%:*', 'All Actions', actions_table.action)) AS Action
        
        FROM users
        
        JOIN users_managed_policies ON users_managed_policies.userarn = users.arn
        LEFT JOIN managed_policies ON managed_policies.arn=users_managed_policies.policyarn
        LEFT JOIN managed_policy_actions ON managed_policy_actions.policyarn=managed_policies.arn
        LEFT JOIN actions_table ON actions_table.service_action_combined LIKE managed_policy_actions.action_formatted
        """
        
        if len(service_list) > 0 or len(actions_list) > 0 or len(accesslvl_list) > 0:
            sql += "\nWHERE"
            sql += "\n("
            if len(service_list) > 0:
                sql += "\nmanaged_policy_actions.action_raw LIKE '*' \nOR\n"
                for i, service in enumerate(service_list):
                    sql += "managed_policy_actions.action_formatted LIKE \"{}:%\"".format(service)
                    if i < (len(service_list)-1):
                        sql += "\nOR\n"
                    else:
                        sql += "\n"
                        
            if len(actions_list) > 0:
                if len(service_list) > 0: sql += "OR\n"
                for i, action in enumerate(actions_list):
                    sql += "actions_table.service_action_combined LIKE \"{}\"".format(action)
                    if i < (len(actions_list)-1):
                        sql += "\nOR\n"
                    else:
                        sql += "\n"
            sql += ")"
            
            if len(accesslvl_list) > 0:
                if len(service_list) > 0 or len(actions_list) > 0: sql += "\nAND"
                sql += "\n("
                for i, action in enumerate(accesslvl_list):
                    sql += " actions_table.access_level LIKE \"{}\"".format(action)
                    if i < (len(accesslvl_list)-1):
                        sql += " OR"
                    else:
                        sql += ")"
        print(sql)
        print('\nQuery is running')
        df_usersByAttachedPolicy = pd.read_sql_query(sql,self.iam.conn)
        print('\nQuery complete')
        
        #query to get users with privileges assigned by user inline policies
        sql = """SELECT DISTINCT users.username,
        users_inline_policies.policyname,
        'InlinePolicy' AS policy_type,
        users_inline_policies.effect,
        users_inline_policies.action_raw,
        users_inline_policies.resource,
        users_inline_policies.actiontype,
        IIF(users_inline_policies.action_raw = '*', 'All Levels',
            IIF(users_inline_policies.action_raw LIKE '%:*', 'All Levels', actions_table.access_level)) AS Access_Level,
        users_inline_policies.condition,
        IIF(users_inline_policies.action_raw = '*', 'All Services', actions_table.service) AS Service,
        IIF(users_inline_policies.action_raw = '*', 'All Actions',
            IIF(users_inline_policies.action_raw LIKE '%:*', 'All Actions', actions_table.action)) AS Action
        
        FROM users
        
        JOIN users_inline_policies ON users.arn=users_inline_policies.userarn
        LEFT JOIN actions_table ON actions_table.service_action_combined LIKE users_inline_policies.action_formatted
        """
        
        if len(service_list) > 0 or len(actions_list) > 0 or len(accesslvl_list) > 0:
            sql += "\nWHERE"
            sql += "\n("
            if len(service_list) > 0:
                sql += "\nusers_inline_policies.action_raw LIKE '*' \nOR\n"
                for i, service in enumerate(service_list):
                    sql += " users_inline_policies.action_formatted LIKE \"{}:%\"".format(service)
                    if i < (len(service_list)-1):
                        sql += "\nOR\n"
                    else:
                        sql += "\n"
                        
            if len(actions_list) > 0:
                if len(service_list) > 0: sql += "OR\n"
                for i, action in enumerate(actions_list):
                    sql += "actions_table.service_action_combined LIKE \"{}\"".format(action)
                    if i < (len(actions_list)-1):
                        sql += "\nOR\n"
                    else:
                        sql += "\n"
            sql += ")"
            
            if len(accesslvl_list) > 0:
                if len(service_list) > 0 or len(actions_list) > 0: sql += "\nAND"
                sql += "\n("
                for i, action in enumerate(accesslvl_list):
                    sql += " actions_table.access_level LIKE \"{}\"".format(action)
                    if i < (len(accesslvl_list)-1):
                        sql += " OR"
                    else:
                        sql += ")"
        print(sql)
        print('\nQuery is running')
        df_usersByInlinePolicy = pd.read_sql_query(sql,self.iam.conn)
        print('\nQuery complete')

        #combine results        
        df_full = pd.concat([df_usersByGroupAttachedPolicy,df_usersByGroupInline,df_usersByAttachedPolicy, df_usersByInlinePolicy])
        sheet.range('A1').options(index=False,header=True).value=df_full
        #sheet.range('A10000').options(index=False,header=True).value=df_usersByGroupInline
        #sheet.range('A20000').options(index=False,header=True).value=df_usersByAttachedPolicy
        
#####################something is wrong with the query
    def generate_workbook(self):
        self.wb = xw.Book()
        self.wb.activate(steal_focus=True)
        self.attributes("-topmost", True)
        self.wb.sheets.add('zzz')
        self.wb.sheets['Sheet1'].delete()

    def generate_query_builder(self):
        #if self.wb == None:
        self.generate_workbook()
        self.wb.activate(steal_focus=True)
        self.attributes("-topmost", True)
        
        services_sht = self.wb.sheets.add('Services', before='zzz')
        df = self.iam.services
        df['Included'] = 'No'
        services_sht.range('A1').options(index=False,header=True).value=df
        services_sht.autofit()
        services_sht['B1'].expand().api.Validation.Add(3,1,3,"Yes,No")
        services_tbl = services_sht.tables.add(source=services_sht['A1'].expand(), name='Services')
        
        actions_sht = self.wb.sheets.add('Actions', before='zzz')
        df = self.iam.actions[['service','action','formatted','description','access_level']]
        df['Included'] = 'No'
        actions_sht.range('A1').options(index=False,header=True,expand='table').value=df
        actions_sht.autofit()
        actions_sht['D1'].expand().api.Validation.Add(3,1,3,"Yes,No")
        actions_tbl = actions_sht.tables.add(source=actions_sht['A1'].expand(), name='Actions')
        
        accesslvls_sht = self.wb.sheets.add('Access Levels', before='zzz')
        df = self.iam.access_levels
        df['Included'] = 'No'
        accesslvls_sht.range('A1').options(index=False,header=True).value=df
        accesslvls_sht['B1'].expand().api.Validation.Add(3,1,3,"Yes,No")
        accesslvls_sht.autofit()
        accesslvls_tbl = accesslvls_sht.tables.add(source=accesslvls_sht['A1'].expand(), name='Actions')
        
        #self.wb.sheets['Sheet1'].delete()
        
    def on_closing(self):
        if self.wb is not None:
            try:
                self.wb.close()
            except:
                print("Workbook already closed")
        self.destroy()


if __name__ == '__main__':
    root = App(debug=False)
    root.protocol("WM_DELETE_WINDOW", root.on_closing)
    root.mainloop()
    
'''
#%%
users = pd.read_sql_query('SELECT * FROM users', root.iam.conn)
user_group_membership = pd.read_sql_query('SELECT * FROM user_group_membership', root.iam.conn)
managed_policies = pd.read_sql_query('SELECT * FROM managed_policies', root.iam.conn)
managed_policy_actions = pd.read_sql_query('SELECT * FROM managed_policy_actions', root.iam.conn)
actions_table = pd.read_sql_query('SELECT * FROM actions_table', root.iam.conn)
groups = pd.read_sql_query('SELECT * FROM groups', root.iam.conn)
groups_managed_policies = pd.read_sql_query('SELECT * FROM groups_managed_policies', root.iam.conn)
actions_table = pd.read_sql_query('SELECT * FROM actions_table', root.iam.conn)

#%%
df = users.merge(user_group_membership, how="inner", left_on='arn', right_on='userarn', suffixes=[".users",".user_group_membership"])
df = df.merge(groups, how="left", left_on='groupname', right_on='groupname', suffixes=[".user_group_membership",".groups"])
df = df.merge(groups_managed_policies, how="left", left_on='arn.groups', right_on='grouparn', suffixes=[".user_group_membership",".groups_managed_policies"])
df = df.merge(managed_policies, how="left", left_on='policyarn', right_on='arn', suffixes=[".groups_managed_policies",".managed_policies"])
df = df.merge(managed_policy_actions, how="left", left_on='arn', right_on='policyarn', suffixes=[".managed_policies",".managed_policy_actions"])
#df = df.merge(actions_table, how="left", left_on='arn', right_on='service_action_combined', suffixes=[".managed_policies",".managed_policy_actions"])

df['join'] = 1
actions_table['join'] = 1

df = df.merge(actions_table, on='join').drop('join', axis=1)
#%%

sql = """SELECT DISTINCT users.username,
user_group_membership.groupname,
managed_policies.policyname,
'AttachedManagedPolicy' AS policy_type,
managed_policy_actions.effect,
managed_policy_actions.action_raw,
managed_policy_actions.resource,
managed_policy_actions.actiontype,
actions_table.access_level,
managed_policy_actions.condition,
actions_table.service_action_combined

FROM users

JOIN user_group_membership ON users.arn=user_group_membership.userarn
LEFT JOIN groups ON user_group_membership.groupname=groups.groupname
LEFT JOIN groups_managed_policies ON groups_managed_policies.grouparn=groups.arn
LEFT JOIN managed_policies ON managed_policies.arn=groups_managed_policies.policyarn
LEFT JOIN managed_policy_actions ON managed_policy_actions.policyarn=managed_policies.arn
LEFT JOIN actions_table ON actions_table.service_action_combined LIKE managed_policy_actions.action_formatted || '%'"""

#%%
sql = """
SELECT DISTINCT
    managed_policy_actions.action_service

FROM
    managed_policies
JOIN managed_policy_actions ON managed_policy_actions.policyarn=managed_policies.arn

WHERE
    managed_policies.attachmentcount > 0
    
ORDER BY
    managed_policy_actions.action_service
"""

policies = root.iam.sql_to_df(sql)

#%%
sql = """
SELECT DISTINCT
    action_service

FROM
    roles_inline_policies

ORDER BY
    action_service
"""

policies2 = root.iam.sql_to_df(sql)

#%%
sql = """
SELECT DISTINCT
    managed_policy_actions.action_service

FROM
    managed_policies
JOIN managed_policy_actions ON managed_policy_actions.policyarn=managed_policies.arn

WHERE
    managed_policies.attachmentcount > 0
    
UNION

SELECT DISTINCT
    action_service

FROM
    users_inline_policies
    
UNION

SELECT DISTINCT
    action_service

FROM
    groups_inline_policies

UNION

SELECT DISTINCT
    action_service

FROM
    roles_inline_policies
ORDER BY
    action_service
"""

policies = root.iam.sql_to_df(sql)

'''
