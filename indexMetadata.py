from elasticsearch import Elasticsearch
import csv


# Connect to Elasticsearch running on localhost
es = Elasticsearch(['http://localhost:9200'])

if(not es.indices.exists(index=index_name)):
    es.indices.create(index=index_name)


# Test the connection
if es.ping():
    print("Connected to Elasticsearch")
else:
    print("Could not connect to Elasticsearch")

path1 = './podcasts-no-audio-13GB/spotify-podcasts-2020/podcasts-transcripts/0/0/show_00BnuPjwbyMPxVIM7NimQj/7sHyO8wLeEd1LuxfS8AIls.json'

#
# Reads and indexes the file_path file into index_name
# @string file_path : path of tsv metadata file
# @string index_name : name of index
#
def index_metadata(file_path, index_name):
    bulk_size = 10000
    with open(file_path, 'r',) as metadata:
        reader = csv.DictReader(metadata, delimiter='\t')
        bulk_data = []

        for podcast in reader:
            action = {
                "index": {}
            }
            bulk_data.append(action)
            bulk_data.append(
                {
                    'index': index_name,
                    'source': {
                        'show_name': podcast['show_name'],
                        'show_description': podcast['show_description'],
                        'episode_name':podcast['episode_name'],
                        'episode_description':podcast['episode_description'],
                        'duration':podcast['duration'],
                        'show_filename_prefix':podcast['show_filename_prefix'], 
                        'episode_filename_prefix': podcast['episode_filename_prefix']
                    }
                }
            )

            # 
            if len(bulk_data) >= bulk_size:
                insert_bulk_data(bulk_data)
                bulk_data = []
       
       # Insert 
        if bulk_data:
            insert_bulk_data(bulk_data)
                
def insert_bulk_data(bulk_data):
    try:
        response = es.bulk(index=index_name, body=bulk_data, refresh=True)
        print("Bulk insertion successful:")
    except Exception as e:
        print("Error inserting bulk data:", e)

if __name__ == "__main__":
    index_name = "metadata"
    file_path = '/Users/bhaslum/Documents/KTH/År_4/welcome-to-docker/podcasts-no-audio-13GB/metadata.tsv'
    index_metadata(file_path, index_name)

