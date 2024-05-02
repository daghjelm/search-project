from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import csv, time

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
    es = Elasticsearch(
        'https://localhost:9200',
        basic_auth=['elastic', 'YeY_-u-be2U2oGv7I7n_'],
        ssl_assert_fingerprint=(
            'b3bc39969f4f940e9a1bc02f39792f59142cf20fc9c101fd048578060645912c'
        )
    )

    index_name = "metadata"

    file_path = './podcasts-no-audio-13GB/metadata.tsv'
    index_metadata(es, file_path, index_name)