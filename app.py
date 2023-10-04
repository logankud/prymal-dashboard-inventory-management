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
import plotly.graph_objects as go


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

        # Wait for the query to complete
        state = 'RUNNING'

        while (state in ['RUNNING', 'QUEUED']):
            response = athena_client.get_query_execution(QueryExecutionId = query_execution_id)
            logger.info(f'Query is in {state} state..')
            if 'QueryExecution' in response and 'Status' in response['QueryExecution'] and 'State' in response['QueryExecution']['Status']:
                # Get currentstate
                state = response['QueryExecution']['Status']['State']

                if state == 'FAILED':
                    logger.error('Query Failed!')
                elif state == 'SUCCEEDED':
                    logger.info('Query Succeeded!')
            

        # Retrieve the results
        results_response = athena_client.get_query_results(
            QueryExecutionId=query_execution_id
        )

        # Convert the results to a Pandas DataFrame
        column_info = results_response['ResultSet']['ResultSetMetadata']['ColumnInfo']
        column_names = [info['Name'] for info in column_info]
        rows = results_response['ResultSet']['Rows'][1:]  # Skip the header row

        data = []
        for row in rows:
            values = [field['VarCharValue'] for field in row['Data']]
            data.append(dict(zip(column_names, values)))


        df = pd.DataFrame(data)

        logger.info(f'Length of dataframe returned by Athena: {len(df)}')

        return df


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

DATABASE = 'prymal-analytics'
REGION = 'us-east-1'

# Construct query to pull data by product
# ----

QUERY = f"""SELECT order_date
            , sku_name
            , SUM(qty_sold) as qty_sold 
            FROM shopify_qty_sold_by_sku_daily 
            GROUP BY order_date
            , sku_name
            ORDER BY order_date ASC
            """

# Query datalake
# ----

result_df = run_athena_query(query=QUERY, database=DATABASE, region=REGION)
# Format datatypes
result_df['order_date'] = pd.to_datetime(result_df['order_date']).dt.strftime('%Y-%m-%d')
result_df['qty_sold'] = result_df['qty_sold'].astype(int)


# Initialize Dash app
# ----

app = dash.Dash(__name__)

# Reference the underlying flask app (Used by gunicorn webserver in Heroku production deployment)
# ----

server = app.server 

# QUery product options from Glue database
# ----

PRODUCT_LIST = ['Salted Caramel - Large Bag (320 g)',
                'Cacao Mocha - Large Bag (320 g)',
                'Original - Large Bag (320 g)',
                'Vanilla Bean - Large Bag (320 g)',
                'Butter Pecan - Large Bag (320 g)',
                'Cinnamon Dolce - Large Bag (320 g)']


# Initialize plot
# ----

# Define layout
app.layout = html.Div([
    dcc.Dropdown(PRODUCT_LIST, 
                 PRODUCT_LIST[0], 
                 id='product-dropdown'
                 ),
    dcc.Graph(id='line-chart')
])

# Define callback to update the line chart based on product selection
@app.callback(
    Output('line-chart', 'figure'),
    Input('product-dropdown', 'selected_product')
)
def generate_new_line_chart(selected_product: str):

    filtered_df = result_df.loc[result_df['sku_name']==selected_product]
     
    # Create the plotly line chart
    fig = px.line(filtered_df,
                        x='order_date',
                        y='qty_sold',
                        title=f'Total Qty Sold - {selected_product}')
    

    logger.info(f'UPDATED FIG - {selected_product}')
    logger.info(f'UPDATED FIG DF LENGTH - {len(filtered_df)}')


    return fig



if __name__ == '__main__':
    app.run_server(debug=False, host='0.0.0.0', port=8050)
    