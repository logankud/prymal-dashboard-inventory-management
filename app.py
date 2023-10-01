from dash import Dash, callback, html, dcc
import dash_bootstrap_components as dbc
import numpy as np
import pandas as pd
import dash
from dash import dcc
from dash import html
from dash.dependencies import Input, Output
import gunicorn 
import boto3
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError, PartialCredentialsError, ParamValidationError, WaiterError
import loguru
from loguru import logger
import os
import plotly.express as px

# ---------------------------------------
# FUNCTIONS
# ---------------------------------------

# FUNCTION TO EXECUTE ATHENA QUERY AND RETURN RESULTS
# ----------

def run_athena_query(query:str, database: str, region:str):

        
    # Initialize Athena client
    athena_client = boto3.client('athena', 
                                 region_name=region,
                                 aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
                                 aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])

    # Execute the query
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': database
            },
            ResultConfiguration={
                'OutputLocation': 's3://prymal-ops/athena_query_results/'  # Specify your S3 bucket for query results
            }
        )

        query_execution_id = response['QueryExecutionId']
        logger.info(f"Query submitted. Execution ID: {query_execution_id}")


        state = 'RUNNING'

        while (state in ['RUNNING', 'QUEUED']):
            response = athena_client.get_query_execution(QueryExecutionId = query_execution_id)
            logger.info(f'Query is in {state} state..')
            if 'QueryExecution' in response and 'Status' in response['QueryExecution'] and 'State' in response['QueryExecution']['Status']:
                # Get currentstate
                state = response['QueryExecution']['Status']['State']

                if state == 'FAILED':
                    logger.error('Query Failed!')
                    return False
                elif state == 'SUCCEEDED':
                    logger.info('Query Succeeded!')
                    return True
                
        # Retrieve the query results from S3
        query_results_location = response['ResultConfiguration']['OutputLocation']

        s3 = boto3.client('s3',
                        region_name=region,
                        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
                        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
        

        query_results = s3.get_object(Bucket=query_results_location.split('/')[2], Key='/'.join(query_results_location.split('/')[3:]))
        query_results_body = query_results['Body']

        logger.info(query_results_body)

        return query_results_body


    except ParamValidationError as e:
        logger.error(f"Validation Error (potential SQL query issue): {e}")
        # Handle invalid parameters in the request, such as an invalid SQL query

    except WaiterError as e:
        logger.error(f"Waiter Error: {e}")
        # Handle errors related to waiting for query execution

    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        
        if error_code == 'InvalidRequestException':
            logger.error(f"Invalid Request Exception: {error_message}")
            # Handle issues with the Athena request, such as invalid SQL syntax
            
        elif error_code == 'ResourceNotFoundException':
            logger.error(f"Resource Not Found Exception: {error_message}")
            # Handle cases where the database or query execution does not exist
            
        elif error_code == 'AccessDeniedException':
            logger.error(f"Access Denied Exception: {error_message}")
            # Handle cases where the IAM role does not have sufficient permissions
            
        else:
            logger.error(f"Athena Error: {error_code} - {error_message}")
            # Handle other Athena-related errors

    except Exception as e:
        logger.error(f"Other Exception: {str(e)}")
        # Handle any other unexpected exceptions





# ========================================================================
# Execute Code
# ========================================================================


# Initialize Dash app
app = dash.Dash(__name__)

# QUery product options from Glue database
PRODUCT_LIST = ['Salted Caramel - Large Bag (320 g)',
                'Cacao Mocha - Large Bag (320 g)',
                'Original - Large Bag (320 g)',
                'Vanilla Bean - Large Bag (320 g)',
                'Butter Pecan - Large Bag (320 g)',
                'Cinnamon Dolce - Large Bag (320 g)']

# Define callback to update the line chart based on product selection
@app.callback(
    Output('line-chart', 'figure'),
    Input('product-dropdown', 'value')
)

# FUNCTION TO GENERATE LINE CHART (QTY SOLD PER DAY) FOR A SELECTED PRODUCT
# ----------

def generate_new_line_chart(selected_product:str):

    DATABASE = 'prymal-analytics'
    REGION = 'us-east-1'

    # Construct query to pull selected product's data
    QUERY = f"""SELECT order_date
                , sku_name
                , SUM(qty_sold) as qty_sold 
                FROM shopify_qty_sold_by_sku_daily 
                WHERE sku_name = '{selected_product}' 
                GROUP BY order_date
                , sku_name
                ORDER BY order_date ASC"""


    result_df = run_athena_query(query=QUERY, database=DATABASE, region=REGION)


    
    # Create the line chart figure
    figure = px.line(result_df,
                     x='order_date',
                     y='qty_sold',
                     title=f'Total Qty Sold - {selected_product}')

    return figure

# Define layout
app.layout = html.Div([
    dcc.Dropdown(
        id='product-dropdown',
        options=[{'label': product, 'value': product} for product in PRODUCT_LIST],
        value=PRODUCT_LIST[0]
    ),
    dcc.Graph(id='line-chart',
              figure=generate_new_line_chart())
])


if __name__ == '__main__':
    app.run_server(debug=False, host='0.0.0.0', port=8050)
    