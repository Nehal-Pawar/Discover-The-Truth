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

from config import (aws_access_key, aws_secret_key)
PAGE_SIZE = 20
table_name="mytable6"
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
server = app.server

dynamodb = boto3.resource('dynamodb',aws_access_key_id=aws_access_key ,aws_secret_access_key= aws_secret_key,region_name='us-west-2')
table = dynamodb.Table(table_name)
#response = table.query(IndexName="keyword-rank-index", KeyConditionExpression=Key('keyword').eq('trump'))
def getList(dict): 
    return dict.keys() 

response = table.query(KeyConditionExpression=Key('keyword').eq('2016 presidential election'))
item = response['Items']
#print(getList(item[0]))
#print(item[0])

column=['topic','keyword','date','publication','title','content','author','name','text','id']
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

#{'label': 'Chelsea Manning\'s', 'value': 'Chelsea Manning\'s'}

],

#value='17362'

),
    html.Div(id='dd-output-container'),

    dtgood 

])

@app.callback(

    dash.dependencies.Output('datatablegood', 'data'),
    #dash.dependencies.Output('table_models', 'data'),

    [dash.dependencies.Input('demo-dropdown', 'value')])

def updateoutput(value):

    response = table.query(KeyConditionExpression=Key('keyword').eq(value))
    #item = response['Items']
    # Get the service resource.
    return response['Items']  #'You have selected "{}"'.format(listToStr)

if __name__ == '__main__':
    app.run_server(host='0.0.0.0',debug=True)
