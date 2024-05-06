from elasticsearch import Elasticsearch
import os, time

class Searcher:

    def __init__(self):
        pw = os.environ.get('ELASTIC_PW') 
        ssl = os.environ.get('ELASTIC_SSL')
        self.es = Elasticsearch(
            'https://localhost:9200',
            basic_auth=('elastic', str(pw)),
            ssl_assert_fingerprint=(
                str(ssl)
            )
        )

        self.pointer = 0
        self.current_sections = []
        self.metadatas = {}

    def set_current_sections(self, sections):
        self.current_sections = sections

    def get_next_sections(self, n):
        next_sections = self.current_sections[self.pointer:self.pointer + n]
        self.pointer += n 
        return next_sections

    def concatenate_section_transcripts(self, sections, minutes):
        res = []
        for section in sections:
            if section['_source']['episode_id'] not in self.metadatas:
                self.metadatas[section['_source']['episode_id']] = self.metadata_from_episode(section['_source']['episode_id'])

            transcript = self.get_section_span(int(section['_id']), section['_source']['episode_id'], minutes)

            metadata = self.metadatas[section['_source']['episode_id']]
            res.append({
                'show': metadata['show_name'],
                'episode': metadata['episode_name'],
                'start_time': section['_source']['start_time'],
                'transcript': transcript,
                '_score': section['_score'],
            })

        return res

    def get_next_sections_for_frontend(self, n, minutes=2):
        sections = self.get_next_sections(n)
        return {
                'hits': self.concatenate_section_transcripts(sections, minutes),
                'num_hits': len(self.current_sections),
            }


    # Query entire episode transcripts
    def episodes_from_query(self, query):
        resp = self.es.search(
            index='episode-transcripts',
            pretty=True,
            size = 100,
            query= {'match': {
                        'transcript': {
                            'query': query,
                            # 'fuzziness': 'AUTO',
                            'operator': 'or',
                        }
                    }},
            # query={'match_phrase': {'transcript': query}},
        )

        episodes = map(lambda x: {'id': x['_source']['episode_id'], 'score': x['_score']}, resp['hits']['hits'])

        return list(episodes)

    # get all the sections that are in the episode and match the query
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
                                # 'fuzziness': 'AUTO',
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

    # loop over episodes and get all sections that are in the episode and match the query
    def sections_from_episodes(self, episode_id_score, query):
        sections = []
        for episode in episode_id_score:
            episode_sections = self.get_sections_from_episode(episode['id'], query)

            sections += episode_sections
        
        return sections

    def sections_from_episodes_query(self, episode_id_score, query):
        ids = [str(episode['id']) for episode in episode_id_score]

        resp = self.es.search(
            index='section-transcripts',
            pretty=True,
            size = 1000,
            query={
                'bool': {
                    'must': [
                        {'match': {
                            'transcript': {
                                'query': query,
                                # 'fuzziness': 'AUTO',
                                'operator': 'or',
                            }
                        }},
                        {'terms': { 'episode_id': ids }}
                    ]
                }
            })

        return resp['hits']['hits']

    

    # get all sections that match the query from the sections index
    def sections_from_query(self, query):
        resp = self.es.search(
            index='section-transcripts',
            pretty=True,
            size = 1000,
            query={'match': {
                        'transcript': {
                            'query': query,
                            # 'fuzziness': 'AUTO',
                            'operator': 'or',
                        }
                    }})

        return resp['hits']['hits']

    def rank_sections_only(self, sections):
        sections.sort(key=lambda x: x['_score'], reverse=True)
        return sections

    def get_weighted_score(self, episode_score, section_score, episode_weight=6, section_weight=1):
        return episode_weight * episode_score + section_weight * section_score

    def rank_sections_weighted(self, sections, episode_id_score):
        score_map = self.episode_score_map(episode_id_score)
        sections.sort(
            key=lambda x: self.get_weighted_score(score_map.get(x['_source']['episode_id']), x['_score']), 
            reverse=True
            )

        return sections

    def episode_score_map(self, episode_id_score):
        score_map = {}
        for episode in episode_id_score:
            score_map[episode['id']] = episode['score']
        
        return score_map
    
    def index_of_section(self, section_id, sections):
        for i, section in enumerate(sections):
            if section['_id'] == section_id:
                return i
        return -1
    
    def get_section_span(self, section_id: int, episode_id, n_minutes: int):
        n_minutes = int(n_minutes)
        ids = [str(i) for i in range(section_id - n_minutes, section_id + n_minutes)]
        resp = self.es.search(
            index='section-transcripts',
            pretty=True,
            size = 100,
            query={
                'bool': {
                    'must': [
                        {'term': {'episode_id': episode_id }},
                        {'ids': {
                            'values': ids
                        }},
                    ]
                }
            })

        return '\n'.join(map(lambda x: x['_source']['transcript'], resp['hits']['hits']))

    def metadata_from_episode(self, episode_id):
        resp = self.es.search(
            index='metadata',
            pretty=True,
            query={'match': {'episode_id': episode_id}},
        )

        return resp['hits']['hits'][0]['_source']

    def do_search(self, query, weighted=False):
        self.pointer = 0
        self.current_sections = []
        if weighted:
            episode_id_score = self.episodes_from_query(query)
            sections = self.sections_from_episodes_query(episode_id_score, query)
            sections = self.rank_sections_weighted(sections, episode_id_score)
        else:
            sections = self.sections_from_query(query)
            sections = self.rank_sections_only(sections)

        self.set_current_sections(sections)


if __name__ == '__main__':

    searcher = Searcher()
    s = ' backflip. We rock paper scissors'

    start = time.time()

    searcher.do_search(s, weighted=True)
    sections = searcher.get_next_sections_for_frontend(5, 2)
    for section in sections['hits']:
        print('-----------------------------------------------------')
        # print('Transcript:', section['transcript'])
        print('Show:', section['show'])
        print('Episode:', section['episode'])
        print('Score:', section['_score'])
        print('-----------------------------------------------------')
        print()

    sections = searcher.get_next_sections_for_frontend(5, 2)
    for section in sections['hits']:
        print('-----------------------------------------------------')
        # print('Transcript:', section['transcript'])
        print('Show:', section['show'])
        print('Episode:', section['episode'])
        print('Score:', section['_score'])
        print('-----------------------------------------------------')
        print()
   
    print('Time taken:', time.time() - start)
    
