import dash
import dash_core_components as dcc
import dash_html_components as html
from boto3.dynamodb.conditions import Key, Attr
import boto3
import sys
sys.path.insert(1, '/home/ubuntu/')

from config import (aws_access_key, aws_secret_key)

table_name="mytable5"
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
server = app.server

dynamodb = boto3.resource('dynamodb',aws_access_key_id=aws_access_key ,aws_secret_access_key= aws_secret_key,region_name='us-west-2')
table = dynamodb.Table(table_name)
#response = table.get_item(    Key={        'id':'123'    })

#response = table.query(IndexName="keyword-rank-index", KeyConditionExpression=Key('keyword').eq('trump'))

#response = table.query(KeyConditionExpression=Key('keyword').eq('trump'))
#item = response['Items']
#print(item)
app.layout = html.Div(children=[
    html.H1(children='Hello Dash'),
    

    html.Div(children=""),
    dcc.Dropdown(

id='demo-dropdown',

options=[

{'label': '17362', 'value': '17362'},

{'label': '2', 'value': '17393'},

{'label': 'western-australia', 'value': 'western-australia'},

{'label': 'san-francisco', 'value': 'san-francisco'}

],

value='san-francisco'

),
    html.Div(id='dd-output-container')
])

@app.callback(

    dash.dependencies.Output('dd-output-container', 'children'),

    [dash.dependencies.Input('demo-dropdown', 'value')])

def updateoutput(value):
    response = table.query(KeyConditionExpression=Key('id').eq(value))
    item = response['Items']
# Get the service resource.
    return 'You have selected "{}"'.format(str(item))

if __name__ == '__main__':
    app.run_server(host='0.0.0.0',debug=True)
