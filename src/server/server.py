from flask import Flask, request, jsonify, render_template
from elasticsearch import Elasticsearch

app = Flask(__name__, static_url_path="", static_folder="static", template_folder="templates")

#es = Elasticsearch()

@app.route('/search', methods=['POST'])
def search():
    
    
    #response = es.search(index="your_index_name", body={"query": {"match": {"content": query}}})
    # Process results to include necessary data
    response = {
        "hits": {
            "total": {
            "value": 20,
            "relation": "eq"
            },
            "max_score": 1.3862942,
            "hits": [
                {
                    "_source": {
                        "_index": "asd",
                        "podcast_name": "0", 
                        "transcript": "hej hej hej resultat hej hej",
                        "start_timestamp": "45.0003",
                        "end_timestamp": "80.0003"
                    },
                }, 
                {
                    "_source": {
                        "_index": "asd",
                        "podcast_name": "1", 
                        "transcript": "hejdå hejdå resultat .. hejdå",
                        "start_timestamp": "24.0003",
                        "end_timestamp": "76.0003"
                    },
                }, 
            ]
        }
    }
    results = []
    for hit in response["hits"]["hits"]:
        # Extract the necessary fields from each hit
        source = hit['_source']

        result = {
            "podcast": source['podcast_name'],  # Ensure these field names match your ES mapping
            "start": source['start_timestamp'],
            "end": source['end_timestamp'],
        }
        results.append(result)

    return jsonify({"hits": results})


@app.route('/')
def home():
    return render_template('index.html')

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=9090, debug=True)
