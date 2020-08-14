import threading
import time
from tabulate import tabulate

import pandas as pd

# app.py
from flask import Flask, request, jsonify

app = Flask(__name__)
import pandas as pd
from data_loading.korona_gov_update import get_new_values
from data_loading.database import create_connection, TableProps, clear_table, create_table


def run_fetching_in_background():
    while True:
        update()
        time.sleep(60)


@app.route('/update/')
def update():
    database_connection = create_connection()
    data = pd.DataFrame.from_records([get_new_values()])
    data = data.set_index("timestamp")
    new_table = pd.DataFrame()
    try:
        data.to_sql(TableProps.name, database_connection, if_exists='append')
        new_table = pd.read_sql_table(TableProps.name, database_connection)
    except Exception as e:
        print(e)
    finally:
        database_connection.close()
    return new_table.to_html()


@app.route('/get_data/')
def get_data():
    try:
        database_connection = create_connection()
        data = pd.read_sql_table(TableProps.name, database_connection)
        database_connection.close()
        return data.to_html()
    except Exception as e:
        return f"Failed to provide the requested data. Exception: {e}"


@app.route('/get_data_as_json/')
def get_data_as_json():
    database_connection = create_connection()
    data = pd.read_sql_table(TableProps.name, database_connection)
    database_connection.close()
    return data.to_json()


@app.route('/get_data_as_csv/')
def get_data_as_csv():
    database_connection = create_connection()
    data = pd.read_sql_table(TableProps.name, database_connection)
    database_connection.close()
    return data.to_csv()


# A welcome message to test our server
@app.route('/')
def index():
    return f"""<h1>COVID19 Hungary Data Source</h1>
Endpoints: <br>
<ul>
<li> /get_data_as_json/ - to fetch data as json </li>
<li> /get_data_as_csv/ - to fetch data as csv</li>
<li> /get_data/ - to fetch data as html</li> 
<li> /update/ - to trigger an immediate update from gov site</li>
</ul>
"""


if __name__ == '__main__':
    db = create_connection()
    print(type(db))
    try:
        clear_table(db)
    except Exception:
        pass
    try:
        create_table(db)
    except Exception as e:
        print(e)
    db.close()
    # Threaded option to enable multiple instances for multiple user access support
    threading.Thread(target=run_fetching_in_background).start()
    app.run(threaded=True, port=5000, debug=False)
