from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk
import os
import time
from collections import deque
import orjson

def extract_section_data(section_data, item_id, episode, show, index_name):
    return ({
            '_index': index_name,
            '_id': item_id,
            '_source': {
                'transcript': section_data['transcript'], 
                'startTime': section_data['words'][0]['startTime'],
                'endTime': section_data['words'][-1]['endTime'],
                'show'      : show,
                'episode'   : episode
            }
        }
    )

def extract_episode_data(transcript, item_id, episode, show, index_name):
    return ({
            '_index': index_name,
            '_id': item_id,
            '_source': {
                'transcript': transcript,
                'show'      : show,
                'episode'   : episode
            }
        }
    )

def path_wo_ds_store(path):
    dirs = os.listdir(path)[:]
    if '.DS_Store' in dirs:
        dirs.remove('.DS_Store')
    return dirs

def generate_index_data(podcast_path, index_name, dirs=None, letters=None, sections=True):
    start = time.time()
    delta_timer = time.time()
    items = 0
    reads = 0

    if dirs is None:
        dirs = path_wo_ds_store(podcast_path)

    for dir in dirs:
        if letters is None:
            letters = path_wo_ds_store(podcast_path + dir + '/')
        for letter in letters:
            for show in path_wo_ds_store(podcast_path + dir + '/' + letter):
                for episode in path_wo_ds_store(podcast_path + dir + '/' + letter + '/' + show):

                    path = podcast_path + dir + '/' + letter + '/' + show + '/' + episode
                    reads += 1

                    f = open(path, 'rb')
                    data = orjson.loads(f.read())

                    if sections:
                        for section in data['results']:
                            section_data = section['alternatives'][0]
                            if section_data and 'transcript' in section_data:
                                yield extract_section_data(section_data, items, episode.split('.')[0], show, index_name)
                                items += 1
                    else:
                        transcript = ''
                        for section in data['results']:
                            section_data = section['alternatives'][0]
                            if section_data and 'transcript' in section_data:
                                transcript += section_data['transcript']
                        yield extract_episode_data(transcript, items, episode.split('.')[0], show, index_name)
                        items += 1
                    
                    f.close()

                    if reads % 1000 == 0:
                        end = time.time()
                        print(f'took {end - start} (delta {end - delta_timer}) seconds for {items} items and {reads} reads')
                        delta_timer = time.time()

    print('finished in', time.time() - start, 'seconds')
    print('items', items)
    raise StopIteration

def main():
    es = Elasticsearch(
        'https://localhost:9200',
        basic_auth=['elastic', 'xVw89i=t-Be6_eN7-iNS'],
        ssl_assert_fingerprint=(
            '9b9c41a449eedc4ee0b6d4c555fd821213cc4b85755c0121d172c566b9047604'
        )
    )

    index_name = 'episode-transcripts'

    print('deleting...')
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)

    time.sleep(2)

    print('creating...')
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name)

    time.sleep(2)

    path = './podcasts-no-audio-13GB/spotify-podcasts-2020/podcasts-transcripts/'
    dirs = ['0']
    letters = ['A', 'B', 'C', 'D']

    print('starting...') 
    pb = parallel_bulk(
        es, 
        generate_index_data(path, index_name, dirs=dirs, letters=letters, sections=False), 
        chunk_size=1500, 
        thread_count=8
        ) 
    
    deque(pb, maxlen=0)

if __name__ == '__main__':
    main()
