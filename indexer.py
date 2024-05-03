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
                'start_time': section_data['words'][0]['startTime'].split('s')[0],
                'end_time': section_data['words'][-1]['endTime'].split('s')[0],
                'show_id'      : show,
                'episode_id'   : episode
            }})

def extract_episode_data(transcript, item_id, episode, show, index_name):
    return ({
            '_index': index_name,
            '_id': item_id,
            '_source': {
                'transcript': transcript,
                'show_id'      : show,
                'episode_id'   : episode
            }})

def path_wo_ds_store(path):
    dirs = os.listdir(path)[:]
    if '.DS_Store' in dirs:
        dirs.remove('.DS_Store')
    return dirs

def generate_index_data(paths, index_name, sections=True):
    start = time.time()
    delta_timer = time.time()
    items = 0 
    reads = 0

    for podcast_path in paths:
    
        dirs = path_wo_ds_store(podcast_path)

        for dir in dirs:
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

def index_episodes(es, paths, index_name):

    episode_properties = {
        'transcript': {
            'type': 'text',
            'analyzer': 'english'
        },
        'show_id': {
            'type': 'keyword',
        },
        'episode_id': {
            'type': 'keyword'
        }
    }

    es.indices.put_mapping(index=index_name, properties=episode_properties)
    pb = parallel_bulk(
        es, 
        generate_index_data(paths=paths, index_name=index_name, sections=False), 
        chunk_size=1000, 
        thread_count=8
        ) 
    
    deque(pb, maxlen=0)

def index_sections(es, paths, index_name):
    section_properties = {
        'transcript': {
            'type': 'text',
            'analyzer': 'english'
        },
        'start_time': {
            'type': 'float',
        },
        'end_time': {
            'type': 'float',
        },
        'show_id': {
            'type': 'keyword',
        },
        'episode_id': {
            'type': 'keyword'
        }
    }

    es.indices.put_mapping(index=index_name, properties=section_properties)
    pb = parallel_bulk(
        es, 
        generate_index_data(paths=paths, index_name=index_name, sections=True), 
        chunk_size=1500, 
        thread_count=8
        ) 
    
    deque(pb, maxlen=0)

def main():
    pw = os.environ.get('ELASTIC_PW')
    ssl = os.environ.get('ELASTIC_SSL')
    es = Elasticsearch(
        'https://localhost:9200',
        basic_auth=['elastic', pw],
        ssl_assert_fingerprint=(
            ssl
        )
    )

    episode_index_name = 'episode-transcripts'
    section_index_name = 'section-transcripts'

    if not es.indices.exists(index=section_index_name):
        es.indices.create(index=section_index_name)

    if not es.indices.exists(index=episode_index_name):
        es.indices.create(index=episode_index_name)

    paths = [
        "./podcasts-no-audio-13GB/spotify-podcasts-2020/podcasts-transcripts/",
        "./podcasts-no-audio-13GB/spotify-podcasts-2020-3-5/podcasts-transcripts/",
        "./podcasts-no-audio-13GB/spotify-podcasts-2020-6-7/podcasts-transcripts/",
        ]

    print('starting...') 
    index_episodes(es, paths, episode_index_name)

    time.sleep(5)

    index_sections(es, paths, section_index_name)

if __name__ == '__main__':
    main()
