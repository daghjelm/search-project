from elasticsearch import Elasticsearch

# Query entire episode transcripts
def episodes_from_query(es, query):
    resp = es.search(
        index='episode-transcripts',
        pretty=True,
        query={'match_phrase': {'transcript': query}},
    )

    episodes = map(lambda x: {'id': x['_source']['episode_id'], 'score': x['_score']}, resp['hits']['hits'])

    return episodes 

# Query section transripts from a specific episode
def get_sections_from_episode(es, episode_id, query):
    resp = es.search(
        index='section-transcripts',
        pretty=True,
        query={
            'bool': {
                'must': [
                    {'term': {'episode_id': episode_id }},
                    {'match': {'transcript': query }},
                ]
            }
        },
    )

    return resp['hits']['hits']

def get_section_by_id(es, section_id):
    resp = es.search(
        index='section-transcripts',
        pretty=True,
        query={'term': {'_id': section_id}},
    )

    return resp['hits']['hits'][0]['_source']

def sections_from_episodes(episodes, query):
    sections = []
    for episode_id_score in episodes:
        episode_sections = get_sections_from_episode(es, episode_id_score.get(id), query)

        sections += episode_sections
    
    return sections

def rank_sections_only(sections):
    sections.sort(key=lambda x: x['_score'], reverse=True)
    return sections

def get_weighted_score(episode_score, section_score):
    episode_weight = 0.5
    section_weight = 0.5

    return episode_weight * episode_score + section_weight * section_score

def rank_sections_weighted(sections, episode_id_score):
    score_map = episode_score_map(episode_id_score)
    sections.sort(
        key=lambda x: get_weighted_score(score_map.get(x['_source']['episode_id']), x['_score']), 
        reverse=True
        )

    return sections

def episode_score_map(episodes):
    score_map = {}
    for episode in episodes:
        score_map[episode['id']] = episode['score']
    
    return score_map

def ranked_section_from_query(es, query):
    episode_id_score = episodes_from_query(es, query)
    sections = sections_from_episodes(episode_id_score, query)

    return rank_sections_only(sections)

def ranked_section_from_query_weighted(es, query):
    episode_id_score = episodes_from_query(es, query)
    sections = sections_from_episodes(episode_id_score, query)

    return rank_sections_weighted(sections, episode_id_score)

def concatenate_until_time(es, section_id_org: int, n_minute):
    
    desired_length = n_minute * 60
    total_time = 0 
    transcript = ''
    section_id = section_id_org
    iteration = 0
    
    TOTAL_INDEX_SIZE = 5
    
    while (total_time < desired_length):
        
        if iteration == 0:
            offset = 0
        elif iteration % 2 == 1:
            offset = -(iteration // 2 + 1)
        else:
            offset = iteration // 2
            
        section_id = section_id_org + offset
        section = get_section_by_id(es, section_id) 
        
        start = section['start_time']
        end = section['end_time']

        total_time += float(end[0]) - float(start[0])  
        
        if section_id < section_id_org:
            transcript = section['transcript'] + transcript 
        else:
            transcript += section['transcript']
        
        if section_id <= 0 or section_id >= TOTAL_INDEX_SIZE:
            return transcript
        
        iteration += 1
        
    return transcript

def metadata_from_episode(es, episode_id):
    resp = es.search(
        index='metadata',
        pretty=True,
        query={'term': {'episode_id.enum': episode_id}},
    )

    return resp['hits']['hits'][0]['_source']

def section_for_frontend(es, query, minutes):
    sections = ranked_section_from_query(es, query)
    res = []
    for section in sections:
        transcript = concatenate_until_time(es, int(section['_id']), minutes)
        metadata = metadata_from_episode(es, section['_source']['episode_id'])
        res.append({
            'show': metadata['show_name'],
            'episode': metadata['episode_name'],
            'start_time': section['_source']['start_time'],
            'transcript': transcript
        })
    return res
    

if __name__ == '__main__':

    es = Elasticsearch(
        'https://localhost:9200',
        basic_auth=['elastic', 'xVw89i=t-Be6_eN7-iNS'],
        ssl_assert_fingerprint=(
            '9b9c41a449eedc4ee0b6d4c555fd821213cc4b85755c0121d172c566b9047604'
        )
    )

    s = ' backflip. We rock paper scissors'
    # s = 'hey'

    sections = section_for_frontend(es, s, minutes=2)
    print(sections)
   
    