# -*- coding: utf-8 -*-
"""
Created on Mon Mar 11 14:49:30 2019

@author: balima
"""

#%% Import of libraries needed for sql search, dataframe use, bokeh plotting
import pyodbc 
import pandas as pd 

from bokeh.plotting import figure, output_file, show
from bokeh.palettes import Category10
import itertools

#%% database connection
server = 'cfo-sql1' #name of the sql server 
database = 'Lighting' #name of the database in the sql server
username = '' #will use ad3 user and password for computer 
password =  ''

cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';PORT=1443;DATABASE='+database+';UID='+username+';PWD='+ password) #Command for connecting to sql server using pyodbc connect
cursor = cnxn.cursor()

#%% Inputs: Building name, measure type, start date, end date, frequency

#buildings=[
#'GOURLEYCLINICALCNTR',
#'HARTHALL',
#'PLANTENVIROSCIENCE',
#'PLANTREPROBIOFAC',
#'SOCIALSCIENCES',
#'MATHSCIENCE',
#'MEYERHALL',
#'LIFESCIENCE',
#'SCIENCESLAB',
#'VETMED3A',
#'KEMPER',
#'GHAUSI',
#'ACADEMICSURGE',
#'DUTTONHALL'
#]

#meastype = "KW" #measType can be 'KW', 'DAYLIGHT', 'OCCUPANCY','SWITCHLOCK','AFTERHOURS'

#is the database case sensitive?

#reads multiple inputs from user and stored in array
buildings_input = input("Enter Building Name: ")
meastype_input = input("Enter Measuring Type: ")
if (buildings_input == "") or (meastype_input == "") :
    print("Please type building name or measure type.")

split_buildings = buildings_input.split(' ')
split_meastype = meastype_input.split(' ')

start_date = input("Enter Start Date (mm/dd/yyyy): ") 
end_date = input("Enter End Date (mm/dd/yyyy): ")
frequency = input("Enter frequency: ")

#%% Collecting tables that meet the sql search criteria
building_name = []
meastype = []
#does it iterate between building names?
#count number of inputs and then that's how many you iterate through when selecting through sql database

for name in split_buildings:
    for measure in split_meastype:
        building_name = name 
        meastype = measure
        filtered_array = [] #building and measure type array 
        read_sql = "SELECT * from Sys.Tables WHERE name like '%_" + meastype + "%' and name like '%" + building_name + "%'" #sql search that queries based on selected filters
        
        #creating a dataframe with the table names that meet criteria
        with cursor.execute(read_sql): #executes the given database operation
            row = cursor.fetchone() #retrieves next row of query result set and returns single sequence
            while row:
                filtered_array.append({'table': row[0]})
                row = cursor.fetchone()
        #converting table into dataframe with table names of sql search that meets criteria
        filtered_dataframe = pd.DataFrame(filtered_array) 
         
#%% Data extraction
        timerange = pd.date_range(start = start_date, end = end_date, freq = frequency) #setting time range for data collection with data and frequency
        final_dataframe = pd.DataFrame() #beter name for explanation with time filtering
        for i in filtered_dataframe['table']: #change i variable when you figure it out
            
            building = [] 
            #now that table names have been filtered only those will be queried for data in a selected time range
            read_sql = "SELECT [TIMESTAMP],[VALUE] from [dbo].["+i+"] WHERE [TIMESTAMP] BETWEEN "  + "'" + start_date +"'" + " AND " + "'"+end_date+"'"
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
                
                if meastype == 'OCCUPANCY': #occupancy sensor query
                    building = building.reindex(timerange) #originally line 86 commented in and out
                else:
                    building = building.resample(frequency).backfill() #originally line 87 i'm not really sure what this line does, initially freqency = '15min'
        
                #combining all tables queried into one dataframe with the datetime as column
                final_dataframe = pd.concat([building,final_dataframe],axis=1)
                print (i)
            except:
                pass
                
        
        #turning boolean into '1' for on and '0' for off
        final_dataframe *=1
        
        # sort CSV file in numerical order, possible sorting lines
        # room number names on top so organize the room names in numerical order floor first then the room numbers
        # if multiple building names - will sort the building names and then the room numbers also
        
        # final_dataframe.sort_values(["Room Name"], axis = 0, ascending = True, inplace = True)
        # final_dataframe.sort_index(axis = 1, inplace = True)
        final_dataframe.reindex_axis(sorted(final_dataframe.columns, key = lambda x: float(x[1:])), axis = 1)
        
        #exporting to CSV
        final_dataframe.to_csv(buildings_input+'_'+meastype_input+'_DATA_'+'.csv') #changed this to start date for csv name
    
#%% Plot data with bokeh plotting (THIS WILL HAVE MANY ERRORS LOL)
    
    plot_desired = input("Plot? 'Y' or 'N': ")
    
    if plot_desired == 'Y': # user keyboard input for if plot is wanted or not
        
        # iterate through color palette
        def color_gen():
            for c in itertools.cycle(Category10[10]):
                yield c
    
        #graph = figure(plot_width = plot_width, plot_height = plot_height, x_axis_type = 'datetime')
        #auto width/height
        graph = figure(sizing_mode = 'stretch_both', x_axis_type = 'datetime')
    
        colors = color_gen()
        tags = final_dataframe.columns # not sure what the header is called
        for tag in tags:
            color = next(colors)
            graph.circle(final_dataframe.index, final_dataframe[tag], size = 10, color = color, legend = tag) #index: x-axis coordinates tag: y-axis coordinates
            graph.line(final_dataframe.index, final_dataframe[tag], color = color, legend = tag)
        
        graph.legend.click_policy = 'hide'
        
        output_file(buildings_input+'_'+meastype_input+'_DATA_'+'.html') 
    
        show(graph)
