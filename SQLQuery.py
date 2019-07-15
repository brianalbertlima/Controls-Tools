# -*- coding: utf-8 -*-
"""
Created on Mon Mar 11 14:49:30 2019

@author: balima
"""

#%% Import of libraries needed for sql search and dataframe use
import pyodbc
import pandas as pd

#%% database connection
server = 'cfo-sql1' #name of the sql server 
database = 'Lighting' #name of the database in the sql server
username = '' # will use ad3 user and password for computer 
password =  ''

cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';PORT=1443;DATABASE='+database+';UID='+username+';PWD='+ password) #Command for connecting to sql server using pyodbc connect
cursor = cnxn.cursor()





#%% Inputs: Building name and measure type

buildings=[
#'GOURLEYCLINICALCNTR',
#'HARTHALL',
#'PLANTENVIROSCIENCE',
#'PLANTREPROBIOFAC',
#'SOCIALSCIENCES',
#'MATHSCIENCE',
'MEYERHALL',
#'LIFESCIENCE',
#'SCIENCESLAB',
#'VETMED3A',
#'KEMPER',
#'GHAUSI',
#'ACADEMICSURGE',
#'DUTTONHALL'
]

meastype = "KW" #measType can be 'KW', 'DAYLIGHT', 'OCCUPANCY','SWITCHLOCK','AFTERHOURS'

#%% Collecting tables that meet the sql search criteria

for j in buildings:
    building_name=j
    df = []
    read_sql = "SELECT * from Sys.Tables WHERE name like '%_"+ meastype+"%' and name like '%" + building_name + "%'" #sql search that quiries based on selected filters
    
    #creating a dataframe with the table names that meet criteria
    with cursor.execute(read_sql):
        row = cursor.fetchone()
        while row:
            df.append({'table': row[0]})
            row = cursor.fetchone()
    #converting table into dataframe  with table names of sql search that meets criteria
    df3 = pd.DataFrame(df)
    
    
#%% Data extraction
    timerange = pd.date_range(start='4/1/2019',end='7/10/2019', freq='15min') #setting time ranch for data collection with data and frequency
    building2=pd.DataFrame()
    for i in df3['table']:
        
        building = []
        #now that table names have been filtered only those will be quiried for data in a selected time range
        read_sql = "SELECT [TIMESTAMP],[VALUE] from [dbo].["+i+"] WHERE [TIMESTAMP] BETWEEN '04/01/2019' AND '07/10/2019'"
        with cursor.execute(read_sql):
            row = cursor.fetchone()
            while row:
                building.append({'date':row[0],i: row[1]})
                row = cursor.fetchone()
        building = pd.DataFrame(building)
        #Doing a try statement in case the table column is empty 
        try:
            #setting the index column as the date column for data manipulation and because it looks nice
            building = building.set_index('date')
            #filtering so that we only have to the minute resolution
            building.index = building.index.map(lambda x: x.replace(second=0))
            building.index = building.index.map(lambda x: x.replace(microsecond=0))
            #filtering in case of duplicates
            building = building[~building.index.duplicated(keep='first')]
#            building = building.reindex(timerange)
    #        building = building.resample('15min').backfill()
    
            #combining all tables quiried into one dataframe with the datetime as column
            building2 = pd.concat([building,building2],axis=1)
            print (i)
        except:
            pass
            
    
    # turning boolean into '1' for on and '0' for off
    building2 *=1
    
    #exporting to CSV
    building2.to_csv(building_name+'_'+meastype+'_DATA_2019_04_01.csv')