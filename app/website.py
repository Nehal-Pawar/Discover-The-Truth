############################################################
# This python script is a web Dash for Displaying.
# It reads data from DynamoDB 
# Displays it in table format 
# Based on dropdown selection you made
# 
############################################################



# -*- coding:utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html
from boto3.dynamodb.conditions import Key, Attr
import boto3
from dash_table import DataTable
import dash_table
import sys
sys.path.insert(1, '/home/ubuntu/')

#Read aws keys form config file
from config import (aws_access_key, aws_secret_key)
PAGE_SIZE = 20
table_name="mytable6"
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
server = app.server

#create boto3 object to access dynamoDB from AWS
dynamodb = boto3.resource('dynamodb',aws_access_key_id=aws_access_key ,aws_secret_access_key= aws_secret_key,region_name='us-west-2')
table = dynamodb.Table(table_name)
response = table.query(KeyConditionExpression=Key('keyword').eq('Syrian Civil War'))
item = response['Items']
#print(getList(item[0]))
#print(item[0])

# Function to return all keys from table retrived
def getList(dict): 
    return dict.keys() 

# select columns from tabel
column=['topic','keyword','date','publication','title','content','author','name','text','id']

# Display settings
dtgood = dash_table.DataTable(

    id='datatablegood',
    style_data={

        'minWidth': '0px',  'maxWidth': '220px',
        #'whiteSpace': 'normal',
        #'textOverflow': 'ellipsis',

        'height': '50px',
        'overflow': 'hidden'

    },
    style_cell_conditional=[
        {'if': {'column_id': 'date'},
         'maxWidth': '30px'},
    ],
    #style_cell={       'minWidth': '0px',  'maxWidth': '280px',        'overflow': 'hidden',        'textOverflow': 'ellipsis',        'height':'100px'    },

    columns=[{"name":str(i), "id":str(i)} for i in column],

    data=response['Items']

)

# app layout 

app.layout = html.Div( children=[
    dcc.Markdown('''News'''),
    html.H1(children='Discover The Truth'),
    dcc.Dropdown(

id='demo-dropdown',

options=[

{'label': '2016 presidential election', 'value': '2016 presidential election'},

#{'label': 'gunman opens fire', 'value': 'gunman opens fire'},

{'label': '45th President', 'value': '45th President'},

{'label': 'Syrian Civil War', 'value': 'Syrian Civil War'},

],

#value='17362'

),
    html.Div(id='dd-output-container'),

    dtgood 

])

# call back funtion input and output
@app.callback(
    dash.dependencies.Output('datatablegood', 'data'),
    
    [dash.dependencies.Input('demo-dropdown', 'value')])

# function to return table based on selected table 
def updateoutput(value):

    response = table.query(KeyConditionExpression=Key('keyword').eq(value))
    # Get the service resource.
    return response['Items']  #'You have selected "{}"'.format(listToStr)

if __name__ == '__main__':
    app.run_server(host='0.0.0.0',debug=True)
