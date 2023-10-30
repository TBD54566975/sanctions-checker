from flask import Flask, request, jsonify
import us_sdn
import eu_sanctions
from concurrent.futures import ThreadPoolExecutor

app = Flask(__name__)

@app.route('/screen_entity', methods=['POST'])
def screen_entity():
    data = request.json['query']
    # Use ThreadPoolExecutor for parallel execution
    with ThreadPoolExecutor() as executor:
        matched_results_1 = executor.submit(us_sdn.perform_search, data)
        matched_results_2 = executor.submit(eu_sanctions.perform_search, data)

        # Wait for both tasks to complete and retrieve results
        results_1 = matched_results_1.result()
        results_2 = matched_results_2.result()

    # Combine results from both searches (assuming the results are lists)
    combined_hits = results_1.get('hits', []) + results_2.get('hits', [])
    combined_total_hits = len(combined_hits)

    response_data = {
        "hits": combined_hits,
        "total_hits": combined_total_hits
    }

    return jsonify(response_data)

if __name__ == '__main__':
    app.run(debug=True)
