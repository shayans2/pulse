import dash
from dash import dcc, html, Input, Output, State, dash_table
import dash_bootstrap_components as dbc
import plotly.graph_objs as go
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from dash.exceptions import PreventUpdate
from producer import send_to_kafka, start_run_samples_in_background
# from producer import run_samples    start_run_samples_in_background
from dotenv import load_dotenv
import os

# Load env variables
load_dotenv()

# Set default values for environment variables in case they are not set
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')

# Create the connection string
engine = create_engine(f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

# Initialize the Dash app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Expose the Flask server for Gunicorn
server = app.server

app.layout = html.Div([
    dcc.Interval(
        id='interval-component',
        interval=5 * 1000,  # in milliseconds
        n_intervals=0
    ),
    dbc.Container([
        dbc.Row([
            dbc.Col(html.H1("Real-time Dashboard of Comments", className="text-center"),
                    className="mb-5 mt-5")
        ]),
        dbc.Row([
            dbc.Col(dcc.Graph(id='live-update-graph'), width=12)
        ]),
        dbc.Row([
            dbc.Col(
                html.Button('Reset Records', id='reset-button', n_clicks=0),
                width={"size": 6, "offset": 3},
                className="d-grid gap-2"
            )
        ]),
        dbc.Row([
            dbc.Col(
                html.Button('Run Samples', id='samples-button', n_clicks=0),
                width={"size": 6, "offset": 3},
                className="d-grid gap-2"
            )
        ]),
        dbc.Row([
            dbc.Col(dbc.Input(id='input-text', placeholder='Enter text here...', type='text'), width=10),
            dbc.Col(html.Button('Submit', id='submit-button', n_clicks=0), width=2),
        ]),
        dbc.Row([
            dbc.Col(
                dash_table.DataTable(
                    id='table',
                    columns=[{"name": i, "id": i} for i in ['comment', 'classified']],
                    style_table={'height': '300px', 'overflowY': 'auto'},
                    style_cell={'textAlign': 'left'},
                ),
                width=12,
            )
        ]),
    ])
])

@app.callback(
    Output('live-update-graph', 'figure'),
    Input('interval-component', 'n_intervals')
)
def update_graph_live(n):
    df = pd.read_sql("SELECT classified, COUNT(*) as count FROM classified_comments GROUP BY classified", engine)

    # Create a Plotly figure
    figure = {
        'data': [
            go.Bar(
                x=df['classified'],
                y=df['count'],
                marker={'color': df['count'], 'colorscale': 'Viridis'},
            )
        ],
        'layout': {
            'title': 'Count of Positive vs Negative Comments',
            'xaxis': {
                'title': 'Classification',
            },
            'yaxis': {
                'title': 'Count',
            },
        }
    }

    return figure

@app.callback(
    Output('reset-button', 'children'),
    Input('reset-button', 'n_clicks')
)
def reset_records(n_clicks):
    if n_clicks < 1:
        raise PreventUpdate  # No clicks yet, nothing to do

    try:
        with engine.begin() as conn:  # auto-commits or auto-rolls back
            conn.execute("TRUNCATE TABLE classified_comments")
        feedback = "Records reset successfully."
    except SQLAlchemyError as e:
        print(f"Error: {e}")
        feedback = "Failed to reset records."

    return feedback

@app.callback(
    Output('samples-button', 'children'),  # Just to show some feedback
    Input('samples-button', 'n_clicks')
)
def start_samples(n_clicks):
    if n_clicks < 1:
        raise PreventUpdate  # No clicks yet, nothing to do

    start_run_samples_in_background()
    return "Running samples."

@app.callback(
    Output('submit-button', 'children'),  # Just to show some feedback on the button
    Input('submit-button', 'n_clicks'),
    State('input-text', 'value')
)
def handle_dash_input(n_clicks, text):
    if n_clicks < 1 or not text:
        raise PreventUpdate
    topic = os.getenv('KAFKA_TOPIC')
    feedback = send_to_kafka(topic, text)
    return feedback

@app.callback(
    Output('table', 'data'),
    Input('interval-component', 'n_intervals')
)
def update_table(n):
    df_comments = pd.read_sql("SELECT comment, classified FROM classified_comments ORDER BY id DESC LIMIT 10", engine)

    # Return the dataframe as a dictionary in the format Dash DataTable expects
    return df_comments.to_dict('records')

if __name__ == '__main__':
    app.run_server(
        port=8050,
        host='0.0.0.0',
        debug=True
    )
