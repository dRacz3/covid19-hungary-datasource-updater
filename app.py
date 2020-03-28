import pandas as pd

# app.py
from flask import Flask, request, jsonify
app = Flask(__name__)
import pandas as pd
from data_loading.korona_gov_update import get_new_values

savefile = 'covid_data.json'

@app.route('/update/')
def update():
    try:
        data = pd.read_json(savefile)
    except Exception:
        columns = ['Fertztt', 'Gygyult', 'Elhunyt', 'Karantnban', 'Mintavtel']
        data = pd.DataFrame(columns=columns)
    print(data)
    new_data = pd.DataFrame.from_records([get_new_values()]).set_index('timestamp')
    data = pd.concat([data, new_data],  axis=0, join='outer', ignore_index=False)

    data.to_json(savefile)
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
    # Threaded option to enable multiple instances for multiple user access support
    app.run(threaded=True, port=5000)