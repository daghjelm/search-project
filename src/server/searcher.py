from elasticsearch import Elasticsearch

class Searcher:
    def __init__(self):
        self.es = Elasticsearch(
            'https://localhost:9200',
            basic_auth=['elastic', 'xVw89i=t-Be6_eN7-iNS'],
            ssl_assert_fingerprint=(
                '9b9c41a449eedc4ee0b6d4c555fd821213cc4b85755c0121d172c566b9047604'
            )
        )

    # Query entire episode transcripts
    def episodes_from_query(self, query):
        resp = self.es.search(
            index='episode-transcripts',
            pretty=True,
            query= {'match': {
                        'transcript': {
                            'query': query,
                            'fuzziness': 'AUTO',
                            'operator': 'or',
                        }
                    }},
            # query={'match_phrase': {'transcript': query}},
        )

        episodes = map(lambda x: {'id': x['_source']['episode_id'], 'score': x['_score']}, resp['hits']['hits'])

        return episodes 

    # Query section transripts from a specific episode
    def get_sections_from_episode(self, episode_id, query):
        resp = self.es.search(
            index='section-transcripts',
            pretty=True,
            query={
                'bool': {
                    'must': [
                        {'term': {'episode_id': episode_id }},
                        {'match': {
                            'transcript': {
                                'query': query,
                                'fuzziness': 'AUTO',
                                'operator': 'or',
                            }
                        }},
                    ]
                }
            },
        )

        return resp['hits']['hits']

    def get_section_by_id(self, section_id):
        resp = self.es.search(
            index='section-transcripts',
            pretty=True,
            query={'term': {'_id': section_id}},
        )

        return resp['hits']['hits'][0]['_source']

    def sections_from_episodes(self, episodes, query):
        sections = []
        for episode_id_score in episodes:
            episode_sections = self.get_sections_from_episode(episode_id_score['id'], query)

            sections += episode_sections
        
        return sections

    def rank_sections_only(self, sections):
        sections.sort(key=lambda x: x['_score'], reverse=True)
        return sections

    def get_weighted_score(self, episode_score, section_score, episode_weight=0.5, section_weight=0.5):

        return episode_weight * episode_score + section_weight * section_score

    def rank_sections_weighted(self, sections, episode_id_score):
        score_map = self.episode_score_map(episode_id_score)
        sections.sort(
            key=lambda x: self.get_weighted_score(score_map.get(x['_source']['episode_id']), x['_score']), 
            reverse=True
            )

        return sections

    def episode_score_map(self, episodes):
        score_map = {}
        for episode in episodes:
            score_map[episode['id']] = episode['score']
        
        return score_map

    def ranked_section_from_query(self, query):
        episode_id_score = self.episodes_from_query(query)
        sections = self.sections_from_episodes(episode_id_score, query)

        return self.rank_sections_only(sections)

    def ranked_section_from_query_weighted(self, query):
        episode_id_score = self.episodes_from_query(query)
        sections = self.sections_from_episodes(episode_id_score, query)

        return self.rank_sections_weighted(sections, episode_id_score)

    def concatenate_until_time(self, section_id_org: int, episode_id, n_minute):
        
        desired_length = n_minute * 60
        total_time = 0 
        transcript = ''
        section_id = section_id_org
        iteration = 0

        TOTAL_INDEX_SIZE = 124500
        
        while (total_time < desired_length):
            
            if iteration == 0:
                offset = 0
            elif iteration % 2 == 1:
                offset = -(iteration // 2 + 1)
            else:
                offset = iteration // 2
                
            section_id = section_id_org + offset
            section = self.get_section_by_id(section_id) 
            
            if section_id <= 0 or section_id >= TOTAL_INDEX_SIZE or section['episode_id'] != episode_id:
                return transcript
            
            start = section['start_time']
            end = section['end_time']

            total_time += float(end) - float(start)  

            if section_id < section_id_org:
                transcript = section['transcript'] + '\n' + transcript 
            else:
                transcript += '\n' + section['transcript']
            
            iteration += 1
            
        return transcript

    def metadata_from_episode(self, episode_id):
        resp = self.es.search(
            index='metadata',
            pretty=True,
            query={'term': {'episode_id.enum': episode_id}},
        )

        return resp['hits']['hits'][0]['_source']

    def section_for_frontend(self, query, minutes, weighted=False):
        sections = []
        if weighted:
            sections = self.ranked_section_from_query_weighted(query)
        else:
            sections = self.ranked_section_from_query(query) 

        res = []
        for section in sections:
            transcript = self.concatenate_until_time(int(section['_id']), section['_source']['episode_id'], minutes)
            metadata = self.metadata_from_episode(section['_source']['episode_id'])
            res.append({
                'show': metadata['show_name'],
                'episode': metadata['episode_name'],
                'start_time': section['_source']['start_time'],
                'transcript': transcript,
                '_score': section['_score'],
            })
        return res

if __name__ == '__main__':

    searcher = Searcher()
    # s = ' backflip. We rock papr scissors'
    s = ' backflip. We rock paper scissors'
    # s = 'hey'

    sections = searcher.section_for_frontend(s, minutes=2)

    for section in sections[:2]:
        print('-----------------------------------------------------')
        print('Transcript:', section['transcript'])
        print('Show:', section['show'])
        print('Episode:', section['episode'])
        print('Score:', section['_score'])
        print('-----------------------------------------------------')
        print()
   
    