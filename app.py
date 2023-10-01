from dash import Dash, callback, html, dcc
import dash_bootstrap_components as dbc
import numpy as np
import pandas as pd
import matplotlib as mpl
import dash
from dash import dcc
from dash import html
from dash.dependencies import Input, Output

# Initialize the Dash app
app = dash.Dash(__name__)

# Define the layout of the app
app.layout = html.Div([
    html.H1("Basic Dash App"),
    dcc.Input(id="input-box", type="text", value=""),
    html.Div(id="output-div")
])

# Define a callback to update the output based on input
@app.callback(
    Output("output-div", "children"),
    Input("input-box", "value")
)
def update_output(value):
    return f"You entered: {value}"

# Run the app
if __name__ == "__main__":
    app.run_server(debug=True)