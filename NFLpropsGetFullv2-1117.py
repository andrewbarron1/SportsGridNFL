#!/usr/bin/env python
# coding: utf-8

# In[1]:


#Import Statements
#Time

import time
import datetime as dt
from datetime import datetime, timedelta, timezone
from datetime import date
#from datetime import time



#Gspread
from df2gspread import df2gspread as d2g
import gspread
import gspread_dataframe as gd
from oauth2client.service_account import ServiceAccountCredentials

#Pandas
import pandas as pd
import gspread_dataframe as gd
#pd.set_option('max_columns', 250)


#Numpy
import numpy as np
#Requests
import re
import requests

#Beatiful Soup
from bs4 import BeautifulSoup
import bs4 as bs

#UrlLib
import urllib.request
from urllib.request import Request, urlopen
import urllib.request, json

import json
from flatdict import FlatDict

#SQL
from os import environ
import mysql.connector
import sqlalchemy
from sqlalchemy import create_engine

#StatModels
import statsmodels.api as sm
import scipy.stats
import statsmodels.formula.api as smf
import matplotlib.pyplot as plt
from statsmodels.regression.quantile_regression import QuantReg

from scipy.special import gammaln as lgamma

from statsmodels.base.model import GenericLikelihoodModel
from statsmodels.genmod.families import Binomial

import patsy

from functools import reduce
import statsmodels.api as sm
import statsmodels.formula.api as smf
import matplotlib.pyplot as plt
import seaborn as sns 
from statsmodels.regression.quantile_regression import QuantReg
import warnings
import random
import re
warnings.filterwarnings("ignore", category=FutureWarning)
#pd.set_option('max_columns', 200)
import unicodedata
from scipy.stats import poisson
import pytz
import pathlib

# In[2]:

#defining header
header= {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) ' 
      'AppleWebKit/537.11 (KHTML, like Gecko) '
      'Chrome/23.0.1271.64 Safari/537.11',
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
      'Accept-Encoding': 'none',
      'Accept-Language': 'en-US,en;q=0.8',
      'Connection': 'keep-alive'}


# In[3]:


#Let's authorize us to work on that file
directory =  str(pathlib.Path(__file__).parent.resolve())
# print(directory);
timestart =datetime.now(timezone(timedelta(hours=-5), 'EST'))
scope = ['https://spreadsheets.google.com/feeds', "https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive.file", 'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(directory+'/creds.json', scope) 
gc = gspread.authorize(credentials)
#Let's get the next week's worth of Prop Links
sheet = gc.open('NFL End Table')
s1 = sheet.worksheet("PropLinks")
L = pd.DataFrame(s1.get_all_records())
L


# In[4]:


p=L.shape[0]
datap = []
p


# In[5]:


#Loop thru Player Props
for x in range(0,p):
    link=L['Link'][x]
    req = urllib.request.Request(url= link, headers=header)
    with urllib.request.urlopen(req) as url:
        data = json.loads(url.read().decode())
    games=data['sport_schedule_sport_events_players_props']
    

    for game in games:
        start_time=game['sport_event']['start_time']
        home_team=game['sport_event']['competitors'][0]['name']
        away_team=game['sport_event']['competitors'][1]['name']
        
        
        gamedetail= away_team+' at '+home_team
        t1id=game['sport_event']['competitors'][0]['id']
        t2id=game['sport_event']['competitors'][1]['id']
        print(gamedetail)
        try:
            players=game['players_props']
            for player in players:
                PlayerPropID = player['player']['id']
                PlayerPropName = player['player']['name']
                PlayerPropTeamID = player['player']['competitor_id']
                Player_Team =''
                Player_Opp =''
                if PlayerPropTeamID==t1id:
                    Player_Team = home_team
                    Player_Opp = away_team
                elif PlayerPropTeamID==t2id:
                    Player_Team = away_team
                    Player_Opp = home_team
                props= player['markets']
                for prop in props:
                    proptype= prop['name']
                    books=prop['books']
                    for book in books:
                        book_name=book['name']
                        outcomes= book['outcomes']
                        for outcome in outcomes:
                            try:
                                odds=float(outcome['odds_american'])
                                try:
                                    prop_line=float(outcome['total'])
                                    o_u=outcome['type']
                                    if book['removed'] == False:
                                        row=[start_time, home_team, away_team,  PlayerPropName,PlayerPropID, proptype, book_name, odds, o_u, prop_line]
                                        datap.append(row)
                                except:
                                    dummy=3

                            except:
                                dummy=2

        except:
            dummy=1
            
# In[6]:


df=pd.DataFrame(datap)
df.columns = ["start_time", "home_team", "away_team", "player","id","prop", "book", "line", "type", "total"]
df


# In[7]:


#We are going to be updating the Daily ROO data (the Std Deviations for key stats) and storing them.
#The Destination file is: https://docs.google.com/spreadsheets/d/1YAsN1Sez856j2J7sxTV-HBS2oncyAa2QjISy9pN91ZM/edit#gid=0
#or NBARoov1
#Let's authorize us to work on that file
timestart =datetime.now(timezone(timedelta(hours=-5), 'EST'))
scope = ['https://spreadsheets.google.com/feeds', "https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive.file", 'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(directory+'/creds.json', scope) 
gc = gspread.authorize(credentials)

sheet_key = '1GzWBH053ehqkC1iDY2htbBoTdhn8lrpEX3klrVU0BDQ'
spreadsheet_key = '1fHDWyE5ezEDfn-5aIsnGEP4edjfFA-y0Ha4FZ32-srE'


# In[8]:


df['start_time']=pd.to_datetime(df['start_time'])
#df['start_time'] = df['start_time'].dt.tz_localize('utc')
df['start_time'] = df['start_time'].dt.tz_convert('US/Eastern')
now = time.strftime("%Y-%m-%d")
df['start_time_is_today'] = df['start_time'].dt.strftime("%Y-%m-%d") == now

df['full_name'] = df.player.str.split(",").str[::-1].str.join(" ")
df['full_name'] = df['full_name'].str.strip()


# In[9]:


sheet = gc.open("NFL End Table")
w2 = sheet.worksheet("PropBaselines")
w3=w2.get_all_values()
headers=w3.pop(0)
props=pd.DataFrame(w3, columns=headers)

props = props.apply(pd.to_numeric, errors = 'ignore')

props = props.dropna(axis=0, how='any', inplace=False)
props.dtypes


# In[10]:


df2 = df.loc[df['prop'].isin(['total passing yards (incl. overtime)', 'total passing touchdowns (incl. overtime)','total receiving yards (incl. overtime)','total rushing yards (incl. overtime)','total receptions (incl. overtime)','total passing interceptions (incl. overtime)','total carries (incl. overtime)','total passing attempts (incl. overtime)'])]

player_props = pd.merge(df2, props, how = 'inner', left_on = 'full_name', right_on = 'Name')


player_props['projection']=np.where(player_props['prop']=='total passing yards (incl. overtime)', player_props['Pass Yards'], np.where(player_props['prop']=='total receiving yards (incl. overtime)', player_props['Recieving Yards'],np.where(player_props['prop']=='total receptions (incl. overtime)', player_props['Receptions'], 
np.where(player_props['prop']=='total passing touchdowns (incl. overtime)', player_props['Pass TDs'],np.where(player_props['prop']=='total passing interceptions (incl. overtime)', player_props['ProjINT'],np.where(player_props['prop']=='total passing attempts (incl. overtime)', player_props['Pass Attempts'],
np.where(player_props['prop']=='total rushing yards (incl. overtime)', player_props['Rush Yards'],np.where(player_props['prop']=='total carries (incl. overtime)', player_props['Rush Attempts'], 0))))))))


# In[11]:


#Get size of Player Props
p=player_props.shape[0]
data1 = []
p


# In[12]:


for x in range(0,p):
    if player_props['type'][x]== 'over':
        if player_props['prop'][x] =='total passing interceptions (incl. overtime)':
            prob = 1-poisson.cdf(k=player_props['total'][x], mu=player_props['projection'][x])
        elif ((player_props['prop'][x] =='total receiving yards (incl. overtime)')&(player_props['projection'][x]>50)) or ((player_props['prop'][x] =='total rushing yards (incl. overtime)')&(player_props['projection'][x]>50))or((player_props['prop'][x] =='total receptions (incl. overtime)')&(player_props['projection'][x]>4.5)) or((player_props['prop'][x] =='total carries (incl. overtime)')&(player_props['projection'][x]>12.5)):
            prob = 1-scipy.stats.norm.cdf(player_props['total'][x],player_props['projection'][x],.6*player_props['projection'][x])
        elif ((player_props['prop'][x] =='total receiving yards (incl. overtime)')&(player_props['projection'][x]<10)) or ((player_props['prop'][x] =='total rushing yards (incl. overtime)')&(player_props['projection'][x]<10))or ((player_props['prop'][x] =='total receptions (incl. overtime)')&(player_props['projection'][x]<2)) or((player_props['prop'][x] =='total carries (incl. overtime)')&(player_props['projection'][x]<3)):
            prob = 1-scipy.stats.norm.cdf(player_props['total'][x],player_props['projection'][x],2.5*player_props['projection'][x])            
        elif player_props['prop'][x] =='total passing yards (incl. overtime)':
            prob = 1-scipy.stats.norm.cdf(player_props['total'][x],player_props['projection'][x],75)
        else:
            prob = 1-scipy.stats.norm.cdf(player_props['total'][x],player_props['projection'][x],player_props['projection'][x])                                      
    elif player_props['type'][x]== 'under':
        if player_props['prop'][x] =='total passing interceptions (incl. overtime)':
            prob = poisson.cdf(k=player_props['total'][x], mu=player_props['projection'][x])
        elif ((player_props['prop'][x] =='total receiving yards (incl. overtime)')&(player_props['projection'][x]>50)) or ((player_props['prop'][x] =='total rushing yards (incl. overtime)')&(player_props['projection'][x]>50))or ((player_props['prop'][x] =='total receptions (incl. overtime)')&(player_props['projection'][x]>4.5)) or((player_props['prop'][x] =='total carries (incl. overtime)')&(player_props['projection'][x]>12.5)):
            prob = scipy.stats.norm.cdf(player_props['total'][x],player_props['projection'][x],.7*player_props['projection'][x])
        elif ((player_props['prop'][x] =='total receiving yards (incl. overtime)')&(player_props['projection'][x]<10)) or ((player_props['prop'][x] =='total rushing yards (incl. overtime)')&(player_props['projection'][x]<10))or ((player_props['prop'][x] =='total receptions (incl. overtime)')&(player_props['projection'][x]<2)) or((player_props['prop'][x] =='total carries (incl. overtime)')&(player_props['projection'][x]<3)):
            prob = scipy.stats.norm.cdf(player_props['total'][x],player_props['projection'][x],2.7*player_props['projection'][x])            
        elif player_props['prop'][x] =='total passing yards (incl. overtime)':
            prob = scipy.stats.norm.cdf(player_props['total'][x],player_props['projection'][x],77)
        else:
            prob = scipy.stats.norm.cdf(player_props['total'][x],player_props['projection'][x],1.1*player_props['projection'][x])                                      
    else:
        prob = scipy.stats.norm.cdf(player_props['total'][x],player_props['projection'][x],player_props['projection'][x])                                      
    data1.append(prob)
    
     
    
        
        


# In[13]:


player_props['probability']=data1  

player_props['book'] = player_props['book'].apply(lambda x: 'WilliamHill' if 'WilliamHill' in x else x)
player_props = player_props.drop_duplicates()
player_props['book'] = player_props['book'].replace({'SugarHouse.US.NJ' : 'SugarHouse'})

player_props['edge'] = np.where(player_props['line']<0, (((abs(player_props['line'])+100)*player_props['probability']-abs(player_props['line']))/abs(player_props['line'])), (((player_props['line'])+100)*(player_props['probability'])-100)/100)

player_props['STARS'] = np.where(player_props['edge']<.025, 0, np.where(player_props['edge']<.05, 1, np.where(player_props['edge']<.1, 2, np.where(player_props['edge']<.12, 2.5, np.where(player_props['edge']<.15, 3, np.where(player_props['edge']<.2, 3.5, np.where(player_props['edge']<.25, 4, np.where(player_props['edge']<.32, 4.5, 5))))))))
 


# In[14]:



player_props=player_props.rename(columns = {'full_name':'PLAYER NAME', 'Team':'TEAM', 'Opp':'OPPONENT', 'prop':'CATEGORY', 'book':'BOOK', 'line':'BET PRICE', 'type':'BET', 'total':'TOTAL', 'projection':'PROJECTION', 'probability':'PROBABILITY', 'edge':'EDGE'})
player_props = player_props[['start_time','PlayerID','PLAYER NAME', 'TEAM','OPPONENT','CATEGORY', 'BOOK', 'BET PRICE', 'BET', 'TOTAL', 'PROJECTION', 'PROBABILITY','EDGE', 'STARS']]

player_props['CATEGORY']=player_props['CATEGORY'].replace({'total passing interceptions (incl. overtime)':'Interceptions','total passing attempts (incl. overtime)':'Pass Attempts',
                                                           'total passing yards (incl. overtime)':'Passing Yards','total receiving yards (incl. overtime)':'Receiving Yards',
                                                           'total rushing yards (incl. overtime)':'Rushing Yards','total receptions (incl. overtime)':'Receptions',
                                                           'total carries (incl. overtime)':'Carries','total passing touchdowns (incl. overtime)':'Pass TDs'}) 
player_props['BET'] = player_props['BET'].replace({'under':'Under', 'over':'Over'})


# In[ ]:





# In[15]:


engine = sqlalchemy.create_engine('mysql+pymysql://dailyrotodb:QvCPetyGry^201F90!@dailyrotodb.cpvxpuf3txsh.us-east-1.rds.amazonaws.com:3306/dailyrotodb')

player_query = '''SELECT * FROM `NFL_PlayerProps` ORDER BY `NFL_PlayerProps`.`start_time` DESC'''


hp = pd.read_sql(player_query, engine)
hp2 = pd.concat([player_props, hp], ignore_index = True, verify_integrity=True)
hp2 = hp2.drop(['index', 'level_0'], axis=1, errors='ignore')
hp2 = hp2.drop_duplicates(subset=['PLAYER NAME', 'BOOK', 'BET PRICE', 'EDGE'])
hp2.to_sql('NFL_PlayerProps', engine, if_exists='replace')

player_props = player_props[(player_props['STARS'] > 0)]
player_props = player_props[(player_props['PROJECTION'] > 0)&(player_props['TOTAL'] > 0)]
player_props= player_props.drop(['EDGE', 'PROBABILITY'], axis=1)

props = player_props.to_numpy()
mergePlayer = []

data_keys = [
        'game_start_time',
        'id',
        'PlayerName',
        'Team',
        'Opponent',
        'PropType',
        'Books',
        'betPrice',
        'bet',
        'total',
        'Projection',
        #'projection',
        'stars'
    ]

for  pp in props:
    row = [
        datetime.strftime(pp[0],'%m/%d/%Y %I:%M %p') , #game_start_time
        pp[1], #id
        pp[2], #name
        pp[3], #team
        pp[4], #OppPitcher
        pp[5], #category
        pp[6], #bookType
        pp[7], #betPrice
        pp[8], #bet 
        pp[9], #total
        "{:.2f}".format(pp[10]), #projection
        # round(pp[11], 3), #probability
        # round(pp[12], 3),  # edge
        pp[11], #stars
    ]
    mergePlayer.append(dict(zip(data_keys,row)))


storePath = "/home/sportsgrid/public_html/json/player-props/nfl-python.json"


timeZ_et = pytz.timezone('US/Eastern')
date_et = datetime.now(timeZ_et)
timestamp_et = int(round(date_et.timestamp()))

player_props = {'lastUpdated':timestamp_et,'data' : mergePlayer}
with open(storePath, 'w+') as jsonfile:
    json.dump(player_props, jsonfile)
print("data updated...")

