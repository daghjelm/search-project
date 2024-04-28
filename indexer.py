from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, parallel_bulk
import json
import os
import time
from collections import deque
import orjson

def extract_data(section_data, id, episode, show, index_name):
    return ({
            "_index": index_name,
            "_id": id,
            "_source": {
                'transcript': section_data['transcript'], 
                'confidence': section_data['confidence'], 
                'words'     : section_data['words'],
                'show'      : show,
                'episode'   : episode
            }
        }
    )

def path_wo_ds_store(path):
    dirs = os.listdir(path)[:]
    dirs.remove('.DS_Store')
    return dirs

def generate_separate_data(episode_path, items, episode, show, index_name):
    with open(episode_path, "rb") as f:
        data = orjson.loads(f.read())

        for section in data['results']:
            section_data = section['alternatives'][0]
            if section_data and 'transcript' in section_data:
                yield extract_data(section_data, items, episode, show, index_name)
                items += 1

def generate_index_data(podcast_path, index_name):
    start = time.time()
    delta_timer = time.time()
    items = 0
    reads = 0

    # for dir in os.listdir(podcast_path):
    #     if dir == ".DS_Store":
    #         continue
    dir = '0/'
    # letters = ['0/', '1/', '2/', '3/', '4/', '5/', '6/', '7/', '8/', '9/', 'A/', 'B/']
    for letter in path_wo_ds_store(podcast_path + dir):
    # for letter in letters:
        for show in path_wo_ds_store(podcast_path + dir + "/" + letter):
            for episode in path_wo_ds_store(podcast_path + dir + "/" + letter + "/" + show):

                path = podcast_path + dir + "/" + letter + "/" + show + "/" + episode
                reads += 1

                with open(path, "rb") as f:
                    data = orjson.loads(f.read())

                    for section in data['results']:
                        section_data = section['alternatives'][0]
                        if section_data and 'transcript' in section_data:
                            yield extract_data(section_data, items, episode, show, index_name)
                            items += 1

                if reads % 1000 == 0:
                    end = time.time()
                    print(f'took {end - start} (delta {end - delta_timer}) seconds for {items} items and {reads} reads')
                    delta_timer = time.time()

    print('finished in', time.time() - start, 'seconds')
    print('items', items)
    raise StopIteration

def generate_full_transcription_data(path, index_name, episode, show):
    items = 0
    with open(path, "rb") as f:
        data = orjson.loads(f.read())

        for section in data['results']:
            section_data = section['alternatives'][0]
            if section_data and 'transcript' in section_data:
                yield extract_data(section_data, items, episode, show, index_name)
                items += 1



def main():
    es = Elasticsearch(
        "https://localhost:9200",
        basic_auth=["elastic", "xVw89i=t-Be6_eN7-iNS"],
        ssl_assert_fingerprint=(
            "9b9c41a449eedc4ee0b6d4c555fd821213cc4b85755c0121d172c566b9047604"
        )
    )

    index_name = "podcasts"

    print('deleting...')
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)

    time.sleep(2)

    print('creating...')
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name)

    time.sleep(2)

    path = "./podcasts-no-audio-13GB/spotify-podcasts-2020/podcasts-transcripts/"

    print('starting...') 
    # pb = parallel_bulk(es, generate_index_data(path, index_name), chunk_size=2000, thread_count=8) # 389.5941479206085 seconds
    # pb = parallel_bulk(es, generate_index_data(path, index_name), chunk_size=1000, thread_count=8) # 360.2813169956207 seconds
    pb = parallel_bulk(es, generate_index_data(path, index_name), chunk_size=1500, thread_count=8) # 311 seconds
    
    deque(pb, maxlen=0)

if __name__ == "__main__":
    main()
