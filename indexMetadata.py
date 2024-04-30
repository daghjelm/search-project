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
                        'showName': podcast['show_name'],
                        'showDescription': podcast['show_description'],
                        'episodeName': podcast['episode_name'],
                        'episodeDescription': podcast['episode_description'],
                        'duration': podcast['duration'],
                        'showId': podcast['show_filename_prefix'], 
                        'episodeId': podcast['episode_filename_prefix']
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
        basic_auth=['elastic', 'xVw89i=t-Be6_eN7-iNS'],
        ssl_assert_fingerprint=(
            '9b9c41a449eedc4ee0b6d4c555fd821213cc4b85755c0121d172c566b9047604'
        )
    )

    index_name = "metadata"

    file_path = './podcasts-no-audio-13GB/metadata.tsv'
    index_metadata(es, file_path, index_name)