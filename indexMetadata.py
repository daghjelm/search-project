from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import csv, time, os

#
# Reads and indexes the file_path file into index_name
# @string file_path : path of tsv metadata file
# @string index_name : name of index
#
def index_metadata(es, file_path, index_name):
    bulk_size = 10000
    with open(file_path, 'r',) as metadata:
        reader = csv.DictReader(metadata, delimiter='\t')
        bulk_data = []

        for podcast in reader:
            bulk_data.append(
                {
                    '_index': index_name,
                    '_id': podcast['episode_filename_prefix'],
                    '_source': {
                        'show_name': podcast['show_name'],
                        'show_description': podcast['show_description'],
                        'episode_name': podcast['episode_name'],
                        'episode_description': podcast['episode_description'],
                        'duration': podcast['duration'],
                        'show_id': podcast['show_filename_prefix'], 
                        'episode_id': podcast['episode_filename_prefix']
                    }
                }
            )

            # 
            if len(bulk_data) >= bulk_size:
                insert_bulk_data(es, bulk_data)
                bulk_data = []
       
       # Insert 
        if bulk_data:
            insert_bulk_data(es, bulk_data)
                
def insert_bulk_data(es, bulk_data):
    try:
        # response = es.bulk(index=index_name, body=bulk_data, refresh=True)
        bulk(es, bulk_data)
    except Exception as e:
        print("Error inserting bulk data:", e)

if __name__ == "__main__":
    pw = os.environ.get('ELASTIC_PW')
    ssl = os.environ.get('ELASTIC_SSL')

    es = Elasticsearch(
        'https://localhost:9200',
        basic_auth=['elastic', pw],
        ssl_assert_fingerprint=(
            ssl
        )
    )

    index_name = "metadata"

    file_path = './podcasts-no-audio-13GB/metadata.tsv'
    index_metadata(es, file_path, index_name)