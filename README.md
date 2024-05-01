# Podcast Search

The Podcast Search project is a search engine designed to retrieve relevant segments from podcast transcripts.

## Description

The project is built upon Elasticsearch as the search engine to construct the index and provide search functions. The dataset used is Spotify's 2020 transcript available at (https://podcastsdataset.byspotify.com/)

## Getting Started

### Dependencies

1. Install Python 3.11.5
2. Set up venv with `python -m venv .venv`
3. Use new venv with `source path/to/.venv`
4. Install from requirements with `pip install -r requirements.txt`

### Set up Elastic search

1. Download Elastic Search https://www.elastic.co/downloads/elasticsearch
2. Follow instructions https://www.elastic.co/guide/en/elasticsearch/reference/current/targz.html
3. Run Elastic Search with `./path/to/elasticsearch/bin/elasticsearch `
4. Store the elastic search password as an environment variable `export ELASTIC_PASSWORD="your_password"`

### How to index

1. Update `index.py` with correct path to `path/to/dataset`
2. Run elastic search
3. Start indexing `python indexer.py`

### Run the app

1. Run app `python frontend.py`
