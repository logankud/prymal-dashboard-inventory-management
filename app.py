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
            

        # OBTAIN DATA

        # --------------



        query_results = athena_client.get_query_results(QueryExecutionId=query_execution_id,
                                                MaxResults= 1000)
        
        logger.info(query_results)


        # Extract qury result column names into a list  

        cols = query_results['ResultSet']['ResultSetMetadata']['ColumnInfo']
        col_names = [col['Name'] for col in cols]



        # Extract query result data rows
        data_rows = query_results['ResultSet']['Rows'][1:]

        print(f'Length of data_rows: {len(data_rows)}')


        # Convert data rows into a list of lists
        query_results_data = [[r['VarCharValue'] for r in row['Data']] for row in data_rows]



        # Paginate Results if necessary
        while 'NextToken' in query_results:
                query_results = athena_client.get_query_results(QueryExecutionId=query_execution_id,
                                                NextToken=query_results['NextToken'],
                                                MaxResults= 1000)



                # Extract quuery result data rows
                data_rows = query_results['ResultSet']['Rows'][1:]


                # Convert data rows into a list of lists
                query_results_data.extend([[r['VarCharValue'] for r in row['Data']] for row in data_rows])



        results_df = pd.DataFrame(query_results_data, columns = col_names)
        
        return results_df


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
result_df.columns = ['order_date','sku_name','qty_sold']

logger.info(result_df.head(3))
logger.info(result_df.info())
logger.info(f"Count of NULL RECORDS: {len(result_df.loc[result_df['order_date'].isna()])}")
# Format datatypes
result_df['order_date'] = pd.to_datetime(result_df['order_date'], format='%Y-%m-%d').strftime('%Y-%m-%d')
result_df['qty_sold'] = result_df['qty_sold'].astype(int)

logger.info(f"MIN DATE: {result_df['order_date'].min()}")
logger.info(f"MAX DATE: {result_df['order_date'].max()}")


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
    html.Header("Prymal Inventory Management Dashboard"),
    dcc.Dropdown(options=PRODUCT_LIST, 
                 value=PRODUCT_LIST[0], 
                 id='product-dropdown'
                 ),
    dcc.Graph(id='line-chart')
])

# Define callback to update the line chart based on product selection
@app.callback(
    Output('line-chart', 'figure'),
    Input('product-dropdown', 'value')
)
def sync_output(selected_value: str):


    # Create the plotly line chart
    fig = px.line(result_df.loc[result_df['sku_name']==selected_value],
                        x='order_date',
                        y='qty_sold',
                        title=f'Total Qty Sold - {selected_value}')
    
    fig.update_xaxes(title_text='Order Date', type='category')
    fig.update_yaxes(title_text='Qty Sold')
    
    logger.info(f'UPDATED FIG - {selected_value}')
    logger.info(f"UPDATED FIG DF LENGTH - {len(result_df.loc[result_df['sku_name']==selected_value])}")

    return fig



if __name__ == '__main__':
    app.run_server(debug=False, host='0.0.0.0', port=8050)

    