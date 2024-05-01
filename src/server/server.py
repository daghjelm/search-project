from flask import Flask, request, jsonify, render_template
from elasticsearch import Elasticsearch
from response import response
import re

app = Flask(__name__, static_url_path="", static_folder="static", template_folder="templates")

#es = Elasticsearch()
def find_occurrences(text, query):
    return [match.span() for match in re.finditer(query, text, re.IGNORECASE)] 

def convert_seconds_to_hms(time_str):
    # Remove the 's' at the end and convert to float
    seconds = round(float(time_str.rstrip('s')))
    
    # Calculate hours, minutes, and seconds
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    seconds = seconds % 60
    
    # Format the time string in "hh:mm:ss" format
    return f"{hours:02}:{minutes:02}:{seconds:02}"

@app.route('/search', methods=['POST'])
def search():
    
    #response = es.search(index="your_index_name", body={"query": {"match": {"content": query}}})
    # Process results to include necessary data
    data = request.get_json()  # assuming JSON data
    query = data.get('query', '')  # default to empty string if not provided
    results = []
    for hit in response: #top 50 hits?
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
        results.append(result)

    return jsonify({"hits": results})


@app.route('/')
def home():
    return render_template('index.html')

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=9090, debug=True)
