from flask import Flask, request, jsonify
import pandas as pd
from datetime import datetime, timedelta
from fuzzywuzzy import process
import time
import requests
from io import StringIO

app = Flask(__name__)

def load_data():
    """Loads data from the treasury.gov URL"""
    url = "https://www.treasury.gov/ofac/downloads/sdn.csv"
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        data = StringIO(response.text)
        return pd.read_csv(data)
    except requests.RequestException as e:
        print(f"Error fetching data: {e}")
        return None
    
    

df = load_data()  # Load the data the first time
print("loaded data for us sdn")


def periodic_data_update():
    """Periodically fetches and updates the data every 5 minutes"""
    while True:
        print("Updating data...")
        updated_df = load_data()
        if updated_df is not None:
            global df            
            df = updated_df
        time.sleep(300)  # Sleep for 5 minutes



def perform_search(query_data, df=df):
    """
    Function to perform the search on the dataframe based on the query_data.
    """
    # Parse the query data
    name = query_data['name']
    min_score = int(float(query_data['min_score']) * 100)  # Multiply by 10 and convert to integer
    
    # If DOB and dob_months_range are present in the query, use them for filtering
    if 'dob' in query_data and 'dob_months_range' in query_data:
        dob = datetime.strptime(query_data['dob'], "%Y-%m-%dT%H:%M:%S.%f%z")
        dob_months_range = int(query_data['dob_months_range'])
        dob_start = dob - timedelta(days=dob_months_range * 30)  # Approximate a month as 30 days
        dob_end = dob + timedelta(days=dob_months_range * 30)
        dob_columns = [col for col in df.columns if "DOB" in col.upper()]
        dob_filtered_indices = df[df[dob_columns].apply(lambda x: dob_start <= x <= dob_end, axis=1)].index.tolist()
    else:
        dob_filtered_indices = df.index.tolist()  # If DOB is not provided, consider all rows
    
    # Fuzzy search on the name
    name_matches = process.extractBests(name, df.iloc[:, 1].dropna(), score_cutoff=min_score, limit=1000)
    matched_name_indices = [match[2] for match in name_matches]
    
    # Fuzzy search on the country if provided
    if 'country' in query_data:
        country_matches = process.extractBests(query_data['country'], df.iloc[:, 3].dropna(), score_cutoff=50, limit=1000)        
        
        matched_country_indices = [match[2] for match in country_matches]
        
    else:
        matched_country_indices = df.index.tolist()  # If country is not provided, consider all rows
    
    # Get the intersection of the three lists
    final_matched_indices = list(set(matched_name_indices) & set(dob_filtered_indices) & set(matched_country_indices))
    # Build the results dictionary
    results = {
        "total_hits": len(final_matched_indices),
        "hits": [{"name": df.iloc[i, 1], "country": df.iloc[i, 3]} for i in final_matched_indices]
    }
    
    return results





# You can run the periodic update in a background thread if you want the Flask server to run simultaneously.
from threading import Thread
Thread(target=periodic_data_update).start()


