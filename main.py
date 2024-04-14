from elasticsearch import Elasticsearch
import json

client = Elasticsearch(
    "https://localhost:9200",
    # api_key="yJ-Pfx6ySwicJSnMLcOHNA",
    # verify_certs=False,
    basic_auth=["elastic", "xVw89i=t-Be6_eN7-iNS"],
    ssl_assert_fingerprint=(
        "9b9c41a449eedc4ee0b6d4c555fd821213cc4b85755c0121d172c566b9047604"
    )
)

if client.ping():
    print("Connected to Elasticsearch!")
else:
    print("Failed to connect to Elasticsearch.")

def index_doc(path):
    file = open(path, 'r')
    data = json.load(file)
    sections = data['results']
    id = 0
    for section in sections:
        section_data = section['alternatives'][0]
        if section_data:
            client.index(index="podcasts", id=id, body=section_data)
            id += 1

if __name__ == "__main__":
    # podcasts_path = "./podcasts-no-audio-13GB/spotify-podcasts-2020/podcasts-transcripts/0/0/show_002B8PbILr169CdsS9ySTH"
    # podcast_path = podcasts_path + "/399kdfMnjw0KYANZU7CQJ0.json"
    # index_doc(podcast_path)

    # client.bulk(operations=clean_sections, index="podcasts")
    print(client.search(index="podcasts", q="cried on spot")['hits']['hits'][0]['_source']['transcript'])