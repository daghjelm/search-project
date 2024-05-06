from flask import Flask, request, jsonify, render_template
from elasticsearch import Elasticsearch as es
from response import response
from searcher import Searcher
import re, time

app = Flask(__name__, static_url_path="", static_folder="static", template_folder="templates")
searcher = Searcher()
all_results = []

#es = Elasticsearch()
def find_occurrences(text, query):
    words = query.split()  # Splits the query into individual words
    indices = []
    for word in words:
        indices.extend([match.span() for match in re.finditer(r'\b' + re.escape(word) + r'\b', text, re.IGNORECASE)])
    return indices

 
def convert_seconds_to_hms(time_str):
    # Remove the 's' at the end and convert to float
    seconds = round(float(time_str.rstrip('s')))
    
    # Calculate hours, minutes, and seconds
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    seconds = seconds % 60
    
    # Format the time string in "hh:mm:ss" format
    return f"{hours:02}:{minutes:02}:{seconds:02}"

def get_next_from_searcher(data):
    query = data.get('query', '')  # default to empty string if not provided
    minutes = data.get('minutes', 2)  # default to 2 minutes if not provided

    response = searcher.get_next_sections_for_frontend(10, minutes)
    results = []
    for hit in response['hits']: #top 50 hits?
        # Find all occurrences of the query in the transcript
        indices = find_occurrences(hit['transcript'], query)
        # Extract the necessary fields from each hit
        result = {
            "podcast": hit['show'],
            "episode": hit["episode"], 
            "transcript": hit['transcript'],
            "startTime": convert_seconds_to_hms(hit['start_time']),
            #"endTime": convert_seconds_to_hms(hit['end_time']),
            "indices": indices
        }
        all_results.append(result)

    return jsonify({"hits": all_results, 'numHits': response['num_hits']})


@app.route('/search', methods=['POST'])
def search():
    all_results.clear()
    #response = es.search(index="your_index_name", body={"query": {"match": {"content": query}}})
    # Process results to include necessary data
    data = request.get_json()  # assuming JSON data
    query = data.get('query', '')  # default to empty string if not provided
    weighted = data.get('weighted', True) == 'True'

    searcher.do_search(query, weighted=weighted)
    # response = searcher.section_for_frontend(query, int(minutes), weighted)
    return get_next_from_searcher(data)

@app.route('/get-next', methods=['POST'])
def get_next():
    data = request.get_json()  # assuming JSON data
    return get_next_from_searcher(data)

@app.route('/')
def home():
    return render_template('index.html')

if __name__ == "__main__":

    app.run(host='localhost', port=9090, debug=True)
