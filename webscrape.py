# -*- coding: utf-8 -*-
"""
Created on Fri Jun 11 14:50:41 2021

@author: jkuhnsman
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd

class AwsActionTable():
    def __init__(self):
        print('initializing AwsActionTable')
        self.tables = {}
        self.full_table = None
        
        try:
            print('attempt to load the actions table')
            self.load_csv('actions_table.csv')
        except:
            print('actions table does not exist; create a new one')
            self.urls = self.get_urls()
            self.populate_table()
            self.save_csv('actions_table.csv')

        
    def get_urls(self):
        URL = 'https://docs.aws.amazon.com/service-authorization/latest/reference/reference_policies_actions-resources-contextkeys.html'
        page = requests.get(URL)
        soup = BeautifulSoup(page.content, 'html.parser')
        result = soup.find(id='main-column')
        
        hrefs = result.find_all('a')
        
        urls = []
        for link in hrefs:
            if './list_' in link['href']:
                #print(link['href'])
                #print(link.text)
                urls.append('https://docs.aws.amazon.com/service-authorization/latest/reference/'+link['href'][2:])
        return urls        
        
    def get_actions(self,_url):
        #URL = 'https://docs.aws.amazon.com/service-authorization/latest/reference/list_awsaccounts.html'
        page = requests.get(_url)
        soup = BeautifulSoup(page.content, 'html.parser')
        result = soup.find(id='main')
        code = result.find_all('code')
        
        divs = result.find_all('div', class_="table-contents")
        
        tbl = divs[0]
        
        t_headers = []
        
        for th in tbl.find_all("th"): t_headers.append(th.text.replace('\n', '').strip())
        
        table_data = []
        
        for tr in tbl.find_all("tr"):
            t_row = {}
            for td, th in zip(tr.find_all("td"), t_headers):
                txt = td.text.replace('\n', ' ').strip()
                txt = txt.replace('\t', ' ')
                t_row[th] = td.text.replace('\n', '').strip()
            t_row['prefix'] = code[0].text
            table_data.append(t_row)
            
        df = pd.DataFrame(table_data).dropna()
        df.columns = ['service','action','description','access_level','resource_types','condition_keys','dependent_actions']
        df['action'] = df['action'].str.split(' ').str[0]
        df['description'] = df['description'].str.replace('  ','')
            
        return code[0], df

    def populate_table(self):
        for url in self.urls:
            cd, service_df = self.get_actions(url)
            print(cd.text)
            self.tables[cd.text] = service_df
        
        lst = list(self.tables.values())
        df = pd.concat(lst, ignore_index=True)
        df['service_action_combined'] = df['service']+':'+df['action']
        self.full_table = df
            
    def load_csv(self, fname):
        self.full_table = pd.read_csv('actions_table.csv')
        
    def save_csv(self, fname):
        self.full_table.to_csv(fname, index=False)
        
        
