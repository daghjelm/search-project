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

def episode_from_query(es, query):
    resp = es.search(
        index="transcripts",
        pretty=True,
        body={"query": {"match_phrase": {"transcript": query}}},
    )

    first_hit = resp["hits"]["hits"][0]
    return first_hit['_source']['episode']

def get_words_from_episode(es, episode, query):
    resp = es.search(
        index="words",
        pretty=True,
        body={"query": {"match": {"episode": episode_id}}},
    )

    return resp["hits"]["hits"]

if __name__ == "__main__":
    phrase_query = {
        "query": {
            "match_phrase" : {
                "field": "SEarch"
            }
        }
    }
    intersection_query = {
        "query": {
            "bool": {
                "must": [
                    {"match": {"transcript": "Alice"}},
                    {"match": {"time_stamp": "13:37"}}
                ]
            }
        }
    }

    s = "We don't know what it is or where it's going yet"

    episode = episode_from_query(es, s)

    # ------------------------------------
    # pseudo code for getting final results
    # ------------------------------------

    # def get_time_of_word(words, match):
    #     for word in words:
    #         if word == match:
    #             return word.start_time
    #     return None

    # search_term = "I almost cried on the spot"
    # id = resp.id 
    # podcast_name = resp.podcast_name
    # start_time = get_time_of_word(resp.words, search_term[0])
    # returned_transcript = client.get(id - 1).transcript + resp.transcript + client.get(id + 1).transcript
    # returned_start_time = start_time

    # --------------------------------
    # pseudo code ends
    # ------------------------------