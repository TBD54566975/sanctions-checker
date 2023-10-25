from flask import Flask, request, jsonify
import pandas as pd
from datetime import datetime, timedelta
from fuzzywuzzy import process

app = Flask(__name__)

# Load the data
df = pd.read_csv('sdn.csv')

def perform_search(query_data, df):
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
    name_matches = process.extractBests(name, df.iloc[:, 1].dropna(), score_cutoff=min_score)
    matched_indices = [match[2] for match in name_matches]
    
    # Get the intersection of the two lists
    final_matched_indices = list(set(matched_indices) & set(dob_filtered_indices))
    
    # Build the results dictionary
    results = {
        "total_hits": len(final_matched_indices),
        "hits": [{"name": df.iloc[i, 1]} for i in final_matched_indices]
    }
    
    return results

@app.route('/screen_entity', methods=['POST'])
def screen_entity():
    data = request.json
    matched_results = perform_search(data['query'], df)
    return jsonify(matched_results)

if __name__ == '__main__':
    app.run(debug=True)
