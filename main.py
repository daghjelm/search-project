from elasticsearch import Elasticsearch
import json

es = Elasticsearch(
    "https://localhost:9200",
    basic_auth=["elastic", "xVw89i=t-Be6_eN7-iNS"],
    ssl_assert_fingerprint=(
        "9b9c41a449eedc4ee0b6d4c555fd821213cc4b85755c0121d172c566b9047604"
    )
)

if es.ping():
    print("Connected to Elasticsearch!")
else:
    print("Failed to connect to Elasticsearch.")

# Query entire episode transcripts
def episode_from_query(es, query):
    resp = es.search(
        index="episode-transcripts",
        pretty=True,
        query={"match_phrase": {"transcript": query}},
    )

    return resp["hits"]["hits"]

# Query section transripts from a specific episode
def get_words_from_episode(es, episode_id, query):
    resp = es.search(
        index="section-transcripts",
        pretty=True,
        query={
            "bool": {
                "must": [
                    {"term": {"episode.keyword": episode_id}},
                    {"match": {"transcript": query }},
                ]
            }
        },
    )

    return resp["hits"]["hits"]

def get_section_by_id(es, section_id):
    resp = es.search(
        index="section-transcripts",
        pretty=True,
        query={"term": {"_id": section_id}},
    )

    return resp["hits"]["hits"][0]['_source']

def concatenate_until_time(es, section_id, start_time, snipped_length):
    pointer = 1
    total_time = 0
    transcript = ""
    direction = 1
    while (total_time < snipped_length):
        section = get_section_by_id(es, section_id + pointer * direction)
        total_time = section['words'][-1]['endTime'] - start_time
        transcript += section['transcript']
        direction *= -1
        if direction == 1:
            pointer += 1
    
    return transcript

if __name__ == "__main__":
    s = " backflip. We rock paper scissors"

    episode_hits = episode_from_query(es, s)

    episode_id = episode_hits[0]['_source']['episode']

    section_hits = get_words_from_episode(es, episode_id, s)
    first_hit = section_hits[0]
    first_hit_id = first_hit['_id']

    print(get_section_by_id(es, first_hit_id))

    # for hit in section_hits:
    #     print('hit:')
    #     # print('transcript:', hit['_source']['transcript'])
    #     print('score:', hit['_score'])
    
    # [{
    #     'transcript': '..........',
    #     'show': '',
    #     'episode':'',
    #     'start_time': 0,
    #     'end_time': 0,
    # }]