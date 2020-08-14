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
        time.sleep(10)


@app.route('/update/')
def update():
    database_connection = create_connection()
    try:
        print("Reading data from SQL")
        data = pd.read_sql_table(TableProps.name, database_connection)
        print(tabulate(data, data.columns, tablefmt="pretty"))
        data.append(get_new_values(), ignore_index=True)
    except Exception as e:
        print(f"Reading failed.. {e}")
        data = pd.DataFrame.from_records([get_new_values()])
    data.to_sql(TableProps.name, database_connection, if_exists='append')
    database_connection.close()
    return data.to_html()


@app.route('/get_data/')
def get_data():
    database_connection = create_connection()
    data = pd.read_sql_table(TableProps.name, database_connection)
    database_connection.close()
    return data.to_html()


# A welcome message to test our server
@app.route('/')
def index():
    return "<h1>COVID19 Hungary Data Source</h1>"


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
