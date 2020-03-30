import threading
import time

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
        data = pd.read_sql_table(TableProps.name, database_connection)
        data.append(get_new_values())
    except Exception as e:
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


@app.route('/getmsg/', methods=['GET'])
def respond():
    # Retrieve the name from url parameter
    name = request.args.get("name", None)

    # For debugging
    print(f"got name {name}")

    response = {}

    # Check if user sent a name at all
    if not name:
        response["ERROR"] = "no name found, please send a name."
    # Check if the user entered a number not a name
    elif str(name).isdigit():
        response["ERROR"] = "name can't be numeric."
    # Now the user entered a valid name
    else:
        response["MESSAGE"] = f"Welcome {name} to our awesome platform!!"

    # Return the response in json format
    return jsonify(response)


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
    except:
        pass
    db.close()
    # Threaded option to enable multiple instances for multiple user access support
    threading.Thread(target=run_fetching_in_background).start()
    app.run(threaded=True, port=5000, debug=False)
