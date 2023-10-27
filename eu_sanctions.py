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
    
    # Extract query data
    name = query_data['name']
    country = query_data['country'] if 'country' in query_data else ''
    min_score = int(float(query_data['min_score']) * 100)
    
    # Extract date of birth and range if present
    dob = None
    dob_months_range = None
    if 'dob' in query_data:        
        dob = query_data['dob']
        if 'dob_months_range' in query_data:
            dob_months_range = int(query_data['dob_months_range']) * 30  # Convert to days assuming a month is 30 days

    # Perform the fuzzy search
    results_df = fuzzy_search_grouped(df, name, country, dob, dob_months_range, min_score, min_score)
    
    # Build the results dictionary
    results = {
        "total_hits": len(results_df),
        "hits": results_df[['Combined Names', 'Most Frequent Birthdate', 'Most Frequent Country']].to_dict(orient="records")
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


def fuzzy_search_grouped(df, name, country, birthdate, days_range=400, name_ratio_threshold=60, country_ratio_threshold=60):
    # Group by 'Entity_logical_id' and combine data
    combined_names = df.groupby('Entity_logical_id')['Naal_wholename'].apply(lambda x: ' | '.join(x.dropna().unique()))
    
    # Take any available birthdate
    combined_birthdates = df.groupby('Entity_logical_id')['Birt_date'].apply(lambda x: x.dropna().iloc[0] if not x.dropna().empty else None)
    
    # Combine countries with priority to 'Addr_country'
    def combine_countries(row):
        if pd.notna(row['Addr_country']):
            return row['Addr_country']
        return row['Birt_country']
    
    df['Combined_Country'] = df.apply(combine_countries, axis=1)
    combined_countries = df.groupby('Entity_logical_id')['Combined_Country'].apply(lambda x: x.mode()[0] if not x.mode().empty else x.dropna().iloc[0] if not x.dropna().empty else None)

    grouped_df = pd.DataFrame({
        'Combined_Names': combined_names,
        'Most_Frequent_Birthdate': combined_birthdates,
        'Most_Frequent_Country': combined_countries
    }).reset_index()

    # Filter based on fuzzy matching of name, country, and birthdate
    results = []
    birthdate = datetime.strptime(birthdate, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=None)
    start_date = birthdate - timedelta(days=days_range)
    end_date = birthdate + timedelta(days=days_range)
    
    for index, row in grouped_df.iterrows():
        name_ratio = fuzz.partial_ratio(name.lower(), str(row['Combined_Names']).lower())
        country_ratio = fuzz.ratio(country.lower(), str(row['Most_Frequent_Country']).lower())
        
        # Check if the birthdate is within the specified range
        row_birthdate = row['Most_Frequent_Birthdate']
        if pd.notna(row_birthdate):
            row_birthdate = pd.Timestamp(row_birthdate).to_pydatetime()  # Convert to datetime object
            birthdate_match = start_date <= row_birthdate <= end_date
        else:
            birthdate_match = False
        
        # If name and country ratios are above the given thresholds, and birthdate matches, consider it a match
        if name_ratio > name_ratio_threshold and country_ratio > country_ratio_threshold and birthdate_match:
            results.append((index, row['Combined_Names'], row['Most_Frequent_Birthdate'], row['Most_Frequent_Country'], name_ratio, country_ratio))
    
    # Convert results into a DataFrame for easier viewing
    results_df = pd.DataFrame(results, columns=['Index', 'Combined Names', 'Most Frequent Birthdate', 'Most Frequent Country', 'Name Match Ratio', 'Country Match Ratio'])
    return results_df

if __name__ == '__main__':
    app.run(debug=True)


