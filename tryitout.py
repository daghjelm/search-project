from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, parallel_bulk
import ujson
import os
import time
import asyncio

# Connect to Elasticsearch running on localhost
es = Elasticsearch(['http://localhost:9200'])

index_name = "test1"
if(not es.indices.exists(index=index_name)):
    es.indices.create(index=index_name)


# Test the connection
if es.ping():
    print("Connected to Elasticsearch")
else:
    print("Could not connect to Elasticsearch")



mappings = {
    "properties": {
        "transcriptid": {"type": "integer", "analyzer": "english"},
        "words": {"type": "list", "analyzer": "english"},
        "podcastepisode": {"type": "integer", "analyzer": "english"},
        "podcastID": {"type": "integer", "analyzer": "english"}
    }

}

document = {
    "field1": "value1",
    "field2": 123
}


#path = '/Users/bhaslum/Documents/KTH/År_4/welcome-to-docker/podcasts-no-audio-13GB/spotify-podcasts-2020/podcasts-transcripts/0/0/show_00BnuPjwbyMPxVIM7NimQj/7sHyO8wLeEd1LuxfS8AIls.json'

path1 = './podcasts-no-audio-13GB/spotify-podcasts-2020/podcasts-transcripts/0/0/show_00BnuPjwbyMPxVIM7NimQj/7sHyO8wLeEd1LuxfS8AIls.json'

'''
def index_doc(path):
    file = open(path, 'r')
    data = json.load(file)
    sections = data['results']
    id = 0

    for section in sections:
        section_data = section['alternatives'][0]
        if section_data:
            key = 'transcript'
            if key in section_data.keys():
            #if section_data['transcript'] is not NULL:
                #print(section_data['transcript'])
                new_data = {'transcript': section_data['transcript'], 
                            'confidence': section_data['confidence'], 
                            'words'     : section_data['words']  }
                es.index(index=index_name, id=id, body=new_data)
                id += 1
    file.close()
'''

##


def index_doc(path):
    with open(path, 'r') as file:
        data = ujson.load(file)
        sections = data['results']
        show_name = path[path.rindex('/') + 1 : ]
        id = 0
        bulk_data = []
        for section in sections:
            section_data = section['alternatives'][0]
            if section_data and 'transcript' in section_data:
                #key = 'transcript'
                #if key in section_data.keys():
                    bulk_data.append(
                        {
                            "_index":index_name,
                            "_id": id,
                            "_source": {
                                'transcript': section_data['transcript'], 
                                'confidence': section_data['confidence'], 
                                'words'     : section_data['words'],
                                'show'      : show_name

                            }
                        }
                    )
                    id += 1
        bulk(es, bulk_data)

#
# Traverses through all subdirecteries of root_directory and indexes all files in the directories.
# @string root_directory: Directory containing all (three) unziped transcript folders (podcasts-transcripts)
#
def walk_index_dictionary(root_directory):
    #root_directory = './podcasts-no-audio-13GB/spotify-podcasts-2020/podcasts-transcripts/0'
    root_directory = './spotify-podcasts'
    count = 1
    for root_dirpath, root_dirnames, root_filenames in os.walk(root_directory):
        
        if count % 100 == 0:
            print(f'Index {count} files ')
        for file in root_filenames:
            if file != '.DS_Store':
                file_path = os.path.join(root_dirpath, file)
                index_doc(file_path)
                count += 1
                print(f'Index {count} files ')


async def index_doc_new(path, file_names):
    bulk_data = []
    #show_name = path[path.rindex('/') + 1 : ]
    inner_time = 0
    for file_name in file_names:
        file_path = os.path.join(path, file_name)
        with open(file_path, 'r') as file:
            data = ujson.load(file)
            sections = data['results']
            
            id = 0
            start_time = time.time()
            for section in sections:
                section_data = section['alternatives'][0]
                if section_data:
                    key = 'transcript'
                    if key in section_data.keys():
                        bulk_data.append(
                            {
                                "_index":index_name,
                                "_id": id,
                                "_source": {
                                    'transcript': section_data['transcript'], 
                                    'confidence': section_data['confidence'], 
                                    'words'     : section_data['words'],
                                    'show'      : file_name
                                    #'show'      : show_name # Currently in index

                                }
                            }
                        )
                        id += 1
            inner_time += start_time - time.time()
          
    await bulk(es, bulk_data)

##
# Traverses through all subdirecteries of root_directory and indexes all files in the directories.
# Indexes on show-directory at a time.
# @string root_directory: Path to directory containing all (three) unziped transcript folders (podcasts-transcripts)
#
async def walk_index_dictionary_new(root_directory):
    count = 1
    for root_dirpath, root_dirnames, root_filenames in os.walk(root_directory):
        #print(root_dirpath)
        if len(root_filenames) > 1: # More files than just .DS_Store. Reached bottom of directories
            await index_doc_new(root_dirpath, root_filenames)
            count += len(root_filenames)

            if count % 2 == 0:
                print(f'Index {count} files ')

         

def count_transactions():
    response = es.count(index=index_name)
    document_count = response['count']
    # Print the count of documents
    print("Number of documents in index '{}': {}".format(index_name, document_count))

def delete_index():
    query = {
        "query": {
            "match_all": {}
        }
    }
    # Delete documents by query
    response1 = es.delete_by_query(index=index_name, body=query)


if __name__ == "__main__":

    #delete_index()
    #count_transactions()
    asyncio.run(walk_index_dictionary_new('./spotify-podcasts/podcasts-transcripts-0-2/0'))


