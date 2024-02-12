import dash
from dash import dcc, html, Input, Output, State, dash_table
import dash_bootstrap_components as dbc
import plotly.graph_objs as go
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from producer import send_to_kafka, start_run_samples_in_background
# # from producer import run_samples
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
            dbc.Col(dcc.Graph(id='live-update-graph', animate=True), width=12)
        ]),
        dbc.Row([
            dbc.Alert(
                "Success!",
                id="success-alert",
                is_open=False,
                dismissable=True,
                color="success",
            ),
            dbc.Col(
                html.Button('Reset Records', id='reset-button', n_clicks=0, className="btn btn-lg btn-dark rounded-pill"),
                width={"size": 2},
                className="d-grid gap-2"
            ),
                dbc.Col(
                html.Button('Run Samples', id='samples-button', n_clicks=0, className="btn btn-lg btn-dark rounded-pill"),
                width={"size": 2},
                className="d-grid gap-2"
            )
        ], style={'display': 'flex', 'justifyContent': 'center', 'alignItems': 'center', 'paddingBottom': '32px'}),
        dbc.Row([
            dbc.Col(dbc.Input(id='input-text', placeholder='Enter text here...', type='text'), width=7),
            dbc.Col(html.Button('Submit', id='submit-button', className="btn btn-dark rounded-pill", n_clicks=0), width=2),
        ], style={'display': 'flex', 'justifyContent': 'center', 'alignItems': 'center', 'paddingBottom': '32px'}),
        dbc.Row([
            dbc.Col(
                dash_table.DataTable(
                    id='table',
                    columns=[{"name": i, "id": i} for i in ['comment', 'classified']],
                    style_table={'height': '320px', 'overflowY': 'auto'},
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
    Output('success-alert', 'is_open', allow_duplicate=True),
    Output('success-alert', 'children', allow_duplicate=True),
    Input('reset-button', 'n_clicks'),
    [State('success-alert', 'is_open')],
    prevent_initial_call=True
)
def reset_records(n_clicks, is_open):

    try:
        with engine.begin() as conn:  # auto-commits or auto-rolls back
            conn.execute("TRUNCATE TABLE classified_comments")
        feedback = "Records reset successfully."
    except SQLAlchemyError as e:
        print(f"Error: {e}")
        feedback = "Failed to reset records."

    return True, feedback

@app.callback(
    Output('success-alert', 'is_open', allow_duplicate=True),
    Output('success-alert', 'children', allow_duplicate=True),
    Input('samples-button', 'n_clicks'),
    [State('success-alert', 'is_open')],
    prevent_initial_call=True
)
def start_samples(n_clicks, is_open):

    start_run_samples_in_background()
    return True, "Running samples."

@app.callback(
    Output('success-alert', 'is_open', allow_duplicate=True),
    Output('success-alert', 'children', allow_duplicate=True),
    Input('submit-button', 'n_clicks'),
    State('input-text', 'value'),
    [State('success-alert', 'is_open')],
    prevent_initial_call=True
)
def handle_dash_input(n_clicks, text, is_open):
    topic = os.getenv('KAFKA_TOPIC')
    feedback = send_to_kafka(topic, text)
    return True, feedback

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
