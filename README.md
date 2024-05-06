# Podcast Search

The Podcast Search project is a search engine designed to retrieve relevant segments from podcast transcripts.

## Description

The project is built upon Elasticsearch as the search engine to construct the index and provide search functions. The dataset used is Spotify's 2020 transcript available at (https://podcastsdataset.byspotify.com/)

## Getting Started

### Dependencies

1. Install Python 3.11.5
2. Set up venv with `python -m venv .venv`
3. Use new venv with `source .venv/bin/activate`
4. Install from requirements with `pip install -r requirements.txt`

### Set up Elastic search

1. Download Elastic Search https://www.elastic.co/downloads/elasticsearch
2. Follow instructions https://www.elastic.co/guide/en/elasticsearch/reference/current/targz.html
3. Run Elastic Search with `./path/to/elasticsearch/bin/elasticsearch `
4. When you start elastic you will see the password and ssl fingerprint in the terminal, save these.
5. Store the elastic search password as an environment variable `export ELASTIC_PW=your_password`
6. Store the elastic ssl fingerprint as an environment variable `export ELASTIC_SSL=your_ssl`

### How to index

#### Index podcasts
1. In `indexer.py`, make sure the `paths` array in the main function has the correct podcast path
2. Start elastic as described above
3. Make sure you have the correct `ELASTIC_PW` and `ELASTIC_SSL` in your envs
3. Start indexing with `python indexer.py`

#### Index metadata
1. In `indexMetadata.py`, make sure the `paths` array in the main function has the correct podcast path
2. Start elastic as described above
3. Make sure you have the correct `ELASTIC_PW` and `ELASTIC_SSL` in your envs
3. Start indexing with `python indexer.py`

### Run the app

1. Start the server with `python src/server/server.py`
2. Open your browser at `http://localhost:9090`
