from flask import Flask, request, jsonify
import pandas as pd
from datetime import datetime, timedelta
from fuzzywuzzy import fuzz
import time
import requests
from io import StringIO
import xml.etree.ElementTree as ET

app = Flask(__name__)


def fetch_links():

    import requests

    # Fetch RSS XML content from the URL
    url = "https://webgate.ec.europa.eu/fsd/fsf/public/rss"
    response = requests.get(url)

    # Ensure the request was successful
    response.raise_for_status()

    # Parse the XML
    root = ET.fromstring(response.content)

    # Search for the "CSV - v1.0" item and get the link content
    link_content = None
    for item in root.findall(".//item"):
        title = item.find("title").text
        if title == "CSV - v1.0":
            link_content = item.find("link").text
            break

    # Print the link content
    if link_content:
        return link_content
    else:
        # raise an error if the link is not found
        raise Exception("Item with title 'CSV - v1.0' not found.")        



def load_data():
    """Loads data from the specified EU URL"""
    url = fetch_links()
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
                
        # Use semicolon as delimiter and specify encoding as 'utf-8-sig' to handle BOM
        return pd.read_csv(StringIO(response.text), delimiter=';', encoding='utf-8-sig')

    except requests.RequestException as e:
        print(f"Error fetching data: {e}")
        return None


df = load_data()  # Load the data the first time

def periodic_data_update():
    """Periodically fetches and updates the data every 5 minutes"""
    while True:
        print("Updating data...")
        updated_df = load_data()
        if updated_df is not None:
            global df            
            df = updated_df
        time.sleep(300)  # Sleep for 5 minutes

def perform_search(query_data, df):
    """Function to perform the search on the dataframe based on the query_data."""
    # Parse the query data
    name = query_data['name']
    min_score = int(float(query_data['min_score']) * 100)
    
    # If DOB and dob_months_range are present in the query, use them for filtering
    dob_filtered_indices = df.index.tolist()
    if 'dob' in query_data and 'dob_months_range' in query_data:
        dob = datetime.strptime(query_data['dob'], "%Y-%m-%dT%H:%M:%S.%f%z").replace(tzinfo=None)  # Make it offset-naive
        dob_months_range = int(query_data['dob_months_range'])
        dob_start = dob - timedelta(days=dob_months_range * 30)  # Approximate a month as 30 days
        dob_end = dob + timedelta(days=dob_months_range * 30)
        dob_filtered_indices = df[df['Birt_date'].apply(lambda x: dob_start <= pd.Timestamp(x) <= dob_end)].index.tolist()

    
    # Fuzzy search on the name and filter by score
    matched_indices = [index for index, row in df.iterrows() if fuzz.partial_ratio(name.lower(), str(row['Naal_wholename']).lower()) >= min_score and index in dob_filtered_indices]
    
    # Build the results dictionary
    results = {
        "total_hits": len(matched_indices),
        "hits": [{"name": df.iloc[i]['Naal_wholename']} for i in matched_indices]
    }
    
    return results

@app.route('/screen_entity', methods=['POST'])
def screen_entity():
    data = request.json
    matched_results = perform_search(data['query'], df)
    return jsonify(matched_results)

# You can run the periodic update in a background thread if you want the Flask server to run simultaneously.
from threading import Thread
Thread(target=periodic_data_update).start()

if __name__ == '__main__':
    app.run(debug=True)
