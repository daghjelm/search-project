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
        index="transcripts",
        pretty=True,
        query={"match_phrase": {"transcript": query}},
    )

    return resp["hits"]["hits"]

# Query section transripts from a specific episode
def get_words_from_episode(es, episode_id, query):
    resp = es.search(
        index="words",
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

if __name__ == "__main__":
    s = "We don't know what it is or where it's going yet"

    episode_hits = episode_from_query(es, s)
    print('first episode hit:', episode_hits[0]['_source']['episode'])
    print('---------')

    section_hits = get_words_from_episode(es, episode_hits['episode'], s)
    for hit in section_hits:
        print('hit:')
        print(hit["_source"]["transcript"])
        print('starts at', hit["_source"]["words"][0]['startTime'])
        print('ends at', hit["_source"]["words"][-1]['endTime'])