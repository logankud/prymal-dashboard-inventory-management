from dash import Dash, callback, html, dcc, dash_table
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
from datetime import timedelta


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
        


        # Extract qury result column names into a list  

        cols = query_results['ResultSet']['ResultSetMetadata']['ColumnInfo']
        col_names = [col['Name'] for col in cols]



        # Extract query result data rows
        data_rows = query_results['ResultSet']['Rows'][1:]



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
            , sku
            , sku_name
            , SUM(qty_sold) as qty_sold 
            FROM shopify_qty_sold_by_sku_daily 
            
            WHERE partition_date >= DATE(current_date - interval '120' day)
            GROUP BY order_date
            , sku
            , sku_name
            ORDER BY order_date ASC
            """

# Query datalake to get quantiy sold per sku for the last 120 days
# ----

result_df = run_athena_query(query=QUERY, database=DATABASE, region=REGION)
result_df.columns = ['order_date','sku','sku_name','qty_sold']

logger.info(result_df.head(3))
logger.info(result_df.info())
logger.info(f"Count of NULL RECORDS: {len(result_df.loc[result_df['order_date'].isna()])}")
# Format datatypes & new columns
result_df['order_date'] = pd.to_datetime(result_df['order_date']).dt.strftime('%Y-%m-%d')
result_df['qty_sold'] = result_df['qty_sold'].astype(int)
result_df['week'] = pd.to_datetime(result_df['order_date']).dt.strftime('%Y-%W')

logger.info(f"MIN DATE: {result_df['order_date'].min()}")
logger.info(f"MAX DATE: {result_df['order_date'].max()}")


# Create dataframe of skus sold in the time range
skus_sold_df = result_df.loc[~result_df['sku_name'].isna(),['sku','sku_name']].drop_duplicates()


# Construct query to pull latest inventory details 
# ----

QUERY =f"""with inventory AS (

        SELECT CAST(sku AS VARCHAR) as sku
        , name
        , total_fulfillable_quantity
        FROM shipbob_inventory 
        WHERE partition_date = CAST(DATE(CAST(current_date AS TIMESTAMP) - interval '4' hour) AS VARCHAR)
        )


        SELECT CASE WHEN sku IS NULL THEN 'Not Reported'
            ELSE sku end as sku
        , name
        , SUM(total_fulfillable_quantity)
        FROM inventory 
        GROUP BY CASE WHEN sku IS NULL THEN 'Not Reported'
            ELSE sku end 
        , name

                    """

# Query datalake to get current inventory details for skus sold in the last 120 days
# ----

inventory_df = run_athena_query(query=QUERY, database='prymal', region=REGION)
inventory_df.columns = ['sku','name','inventory_on_hand']

logger.info(inventory_df.head(3))
logger.info(inventory_df.info())

# Format datatypes & new columns
inventory_df['inventory_on_hand'] = inventory_df['inventory_on_hand'].astype(int)



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
                
    html.Header("Prymal Inventory Management Dashboard", 
                style={"textAlign":"center"}),
    dcc.Dropdown(options=PRODUCT_LIST, 
                 value=PRODUCT_LIST[0], 
                 id='product-dropdown'
                 ),
    dcc.Graph(id='inventory-on-hand-indicator'),
    html.Div(id="stockout-date-alert"),
    # html.Table([
    #     html.Tr([html.Td(['Inventory on Hand']), html.Td(id='inventory_on_hand')])
    # ]),
    dcc.Textarea(
        id='text_stockout_date_range',
        value='Forecasted stockout date range: ',
        style={'textAlign':'center','width': '100%', 'height': 50},
    ),
    dash_table.DataTable(id='forecast-table',
                        columns=[{"name": "Forecast", "id": "forecast"},
                                 {"name": "Lower Bound", "id": "lower_bound"},
                                 {"name": "Upper Bound", "id": "upper_bound"}]
),
    dcc.Graph(id='line-chart'),
    dcc.Graph(id='line-chart-weekly')
])


# app.layout = dash.html.Div(
#     [
#         dbc.Row(
#             html.Header("Prymal Inventory Management Dashboard", 
#                 style={"textAlign":"center"}),

#         ),
#         dbc.Row(
#             [
#                 dbc.Col(
#                     dbc.Row(
#                         [
#                             dash.html.Label("SELECT A PRODUCT:"),
#                             dcc.Dropdown(options=PRODUCT_LIST, 
#                                 value=PRODUCT_LIST[0], 
#                                 id='product-dropdown'
#                                 )
#                         ],
#                         justify="center",
#                     ),
#                 ),
#                 dbc.Col(
#                     dbc.Row(
#                         [
#                             html.Table([
#                                 html.Tr([html.Td(['Inventory on Hand']), html.Td(id='inventory_on_hand')])
#                             ]),
#                         ],
#                         justify="center",
#                     ),
#                 ),
#             ],
#             justify="center",
#         ),
        # dbc.Row(
        #     dcc.Textarea(
        #             id='text_stockout_date_range',
        #             value='Forecasted stockout date range: ',
        #             style={'textAlign':'center','width': '100%', 'height': 50},
        #         ),
        #     dash_table.DataTable(id='forecast-table',
        #                             columns=[{"name": "Forecast", "id": "forecast"},
        #                                     {"name": "Lower Bound", "id": "lower_bound"},
        #                                     {"name": "Upper Bound", "id": "upper_bound"}]
        #         ),
        #     dcc.Graph(id='line-chart'),
        #     dcc.Graph(id='line-chart-weekly')

        # ),
#     ],
#     style={"font-family": "Arial", "font-size": "0.9em", "text-align": "center"},
# )


# Define callback to update the line chart based on product selection
@app.callback(
    Output('forecast-table', 'data'),
    Output('inventory-on-hand-indicator', 'figure'),
    Output('stockout-date-range','value'),
    Output('text_stockout_date_range', 'value'),
    Output("stockout-date-alert", "is_open")
    Input('product-dropdown', 'value')
)
def generate_near_future_forecast(selected_value):

    logger.info(f'UPDATING FORECAST TABLE - {selected_value}')

    # DAILY QTY SOLD
    # -----

    # Calculate daily dataframe
    daily_df = result_df.loc[result_df['sku_name']==selected_value].sort_values('order_date',ascending=False)

    # Calculate statistics for past 7, 14, 30 & 60 days
    last_7_median = daily_df.head(7)['qty_sold'].median()
    last_7_p25 = np.percentile(daily_df.head(7)['qty_sold'],25)
    last_7_p75 = np.percentile(daily_df.head(7)['qty_sold'],75)

    last_14_median = daily_df.head(14)['qty_sold'].median()
    last_14_p25 = np.percentile(daily_df.head(14)['qty_sold'],25)
    last_14_p75 = np.percentile(daily_df.head(14)['qty_sold'],75)

    last_30_median = daily_df.head(30)['qty_sold'].median()
    last_30_p25 = np.percentile(daily_df.head(30)['qty_sold'],25)
    last_30_p75 = np.percentile(daily_df.head(30)['qty_sold'],75)

    last_60_median = daily_df.head(60)['qty_sold'].median()
    last_60_p25 = np.percentile(daily_df.head(60)['qty_sold'],25)
    last_60_p75 = np.percentile(daily_df.head(60)['qty_sold'],75)

    # Consolidate stats
    recent_stats_df = pd.DataFrame([[last_7_p25, last_7_median, last_7_p75],
                [last_14_p25, last_14_median, last_14_p75],
                [last_30_p25, last_30_median, last_30_p75],
                [last_60_p25, last_60_median, last_60_p75]],
                columns=['percentile_25','median','percentile_75'])



    # Calculate median of lower bound (median) and upper bound (75th percentile) 
    lower_bound = recent_stats_df['median'].median()
    upper_bound = recent_stats_df['percentile_75'].median()

    # Extrapolate out 30, 60, 90 days
    forecast_30 = ['30-day forecast', lower_bound * 30,upper_bound * 30]
    forecast_60 = ['60-day forecast', lower_bound * 60,upper_bound * 60]
    forecast_90 = ['90-day forecast', lower_bound * 90,upper_bound * 90]

    # Consolidate into dataframe
    df = pd.DataFrame([forecast_30, forecast_60, forecast_90],
                columns=['forecast','lower_bound','upper_bound'])


    logger.info(f"{df.to_dict('records')}")


# -------------------------------------------------------------------------------------------

    logger.info(f'SUBSETTING INVENTORY TABLE - {selected_value}')

    selected_sku = skus_sold_df.loc[skus_sold_df['sku_name']==selected_value,'sku'].values[0]

    # CURRENT INVENTORY ON HAND
    # -----

    inventory_on_hand = inventory_df.loc[inventory_df['sku']==selected_sku,'inventory_on_hand'].values[0]

    logger.info(f'INVENTORY ON HAND: {inventory_on_hand}')

    # Create the plotly indicator chart
    fig = go.Figure()

    fig.add_trace(go.Indicator(
        value = inventory_on_hand)
    )
    
    fig.update_layout(
    template = {'data' : {'indicator': [{
        'title': {'text': "Inventory on Hand"},
        'mode' : "number"}]
                        }
                    }
                )

    # CALCULATE EXPECTED STOCKOUT DATE RANGE

    stockout_days_lower = inventory_on_hand / lower_bound
    stockout_days_upper =inventory_on_hand / upper_bound


    stockout_date_lower = pd.to_datetime(pd.to_datetime('today') + timedelta(stockout_days_lower)).strftime('%Y-%m-%d')
    stockout_date_upper = pd.to_datetime(pd.to_datetime('today') + timedelta(stockout_days_upper)).strftime('%Y-%m-%d')

    logger.info(f"Expected stockout date for {selected_value}: {stockout_date_upper} - {stockout_date_lower}")

    stockout_date_message = f"Forecased Stockout Date Range: {stockout_date_upper} - {stockout_date_lower}"

    return df.to_dict('records'), fig, stockout_date_message, stockout_date_message, dbc.Alert(stockout_date_message, dismissable=False)

# Define callback to update the line chart based on product selection
@app.callback(
    Output('line-chart', 'figure'),
    Input('product-dropdown', 'value')
)
def update_daily_fig(selected_value: str):


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

@app.callback(
    Output('line-chart-weekly', 'figure'),
    Input('product-dropdown', 'value')
)
def update_weekly_fig(selected_value: str):

    weekly_df = result_df.loc[result_df['sku_name']==selected_value].groupby('week',as_index=False)['qty_sold'].sum()

    
    # Create the plotly line chart
    fig = px.line(weekly_df,
                        x='week',
                        y='qty_sold',
                        title=f'Total Qty Sold Weekly - {selected_value}')
    
    fig.update_xaxes(title_text='Order Week', type='category')
    fig.update_yaxes(title_text='Qty Sold')
    
    logger.info(f'UPDATED FIG - {selected_value}')
    logger.info(f"UPDATED FIG DF LENGTH - {len(result_df.loc[result_df['sku_name']==selected_value])}")

    return fig



if __name__ == '__main__':
    app.run_server(debug=False, host='0.0.0.0', port=8050)

    