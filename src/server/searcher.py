from elasticsearch import Elasticsearch
import os, time

class Searcher:
    def __init__(self):
        pw = os.environ.get('ELASTIC_PW') 
        ssl = os.environ.get('ELASTIC_SSL')
        self.es = Elasticsearch(
            'https://localhost:9200',
            basic_auth=['elastic', pw],
            ssl_assert_fingerprint=(
                ssl
            )
        )

    # Query entire episode transcripts
    def episodes_from_query(self, query):
        resp = self.es.search(
            index='episode-transcripts',
            pretty=True,
            size = 100,
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

        return list(episodes)
    
    def episode_section_map(self, episodes):
        resp = self.es.search(
            index='episode-transcripts',
            pretty=True,
            size = 100,
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

    # loop over episodes and get all sections that are in the episode and match the query
    def sections_from_episodes(self, episode_id_score, query):
        sections = []
        for episode in episode_id_score:
            episode_sections = self.get_sections_from_episode(episode['id'], query)

            sections += episode_sections
        
        return sections
    

    # get all sections that match the query from the sections index
    def sections_from_query(self, query):
        resp = self.es.search(
            index='section-transcripts',
            pretty=True,
            size = 100,
            query={'match': {
                        'transcript': {
                            'query': query,
                            'fuzziness': 'AUTO',
                            'operator': 'or',
                        }
                    }})

        return resp['hits']['hits']

    def rank_sections_only(self, sections):
        sections.sort(key=lambda x: x['_score'], reverse=True)
        return sections

    def get_weighted_score(self, episode_score, section_score, episode_weight=6, section_weight=1):
        print(f"Episode score: {episode_score}")
        print(f"Section score: {section_score}")
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
    
    def get_section_span(self, section_id: int, episode_id):
        ids = [str(i) for i in range(section_id - 10, section_id + 10)]
        resp = self.es.search(
            index='section-transcripts',
            pretty=True,
            size = 100,
            query={
                'bool': {
                    'must': [
                        {'term': {'episode_id': episode_id }},
                        {'match': { 'transcript': {
                            'query': ' '.join(ids),
                            'operator': 'or',
                        }}}
                    ]
                }
            })

        return resp['hits']['hits']
    
    def concatenate_with_sections(self, section_id, episode_id, n_minutes):
        section_index = self.index_of_section(section_id, sections) 
        if section_index < 0:
            raise ValueError('Section not found in sections')
        
        sections = self.get_section_span(n_minutes, section_id, episode_id)
        
        n_sections = n_minutes * 2
        #start_index = section_index - (n_sections // 2 - 1)
        start_index = section_index - n_sections // 2
        #end_index = section_index + (n_sections // 2)
        end_index = section_index + n_sections // 2

        # if there isnt enough space backwards in the episode
        if start_index < 0:
            end_index -= start_index
            start_index = section_index
        
        if end_index > len(sections) - 1:
            start_index -= end_index - (len(sections) - 1)
            end_index = len(sections) - 1
        
        transcript = ''

        # print('section_index:', section_index)
        # print('len sectinos', len(sections))
        # print('start_index:', start_index)
        # print('end_index:', end_index)

        for section in sections[start_index:end_index]:
            print(transcript)
            transcript += section['_source']['transcript'] + '\n'

        return transcript

    def concatenate_until_time(self, section_id_org: int, episode_id, n_minute, sections):

        desired_length = n_minute * 60
        total_time = 0 
        transcript = ''
        section_id = section_id_org
        iteration = 0

        # TOTAL_INDEX_SIZE = 8429540 #total
        TOTAL_INDEX_SIZE = 124500 #test
        
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
            query={'match': {'episode_id': episode_id}},
        )

        return resp['hits']['hits'][0]['_source']

    def section_for_frontend(self, query, minutes, weighted=False):
        start = time.time()
        sections = []
        sections_map = {}

        if weighted:
            episode_id_score = self.episodes_from_query(query)
            sections, sections_map = self.sections_from_episodes(episode_id_score, query)
            sections = self.rank_sections_weighted(sections, episode_id_score)
        else:
            sections, sections_map = self.sections_from_query(query)
            sections = self.rank_sections_only(sections)
            """
            episode_id_score = self.episodes_from_query(query)
            sections, sections_map = self.sections_from_episodes(episode_id_score, query)
            sections = self.rank_sections_only(sections)
            """


        print('before stuff time ', time.time() - start)

        res = []
        transcript_time = 0
        metadata_time = 0
        
        metadatas = {}
        for section in sections:
            # transcript = self.concatenate_until_time(int(section['_id']), section['_source']['episode_id'], minutes)
            loop_start = time.time()
            transcript = self.concatenate_with_sections(section['_id'], minutes, sections_map[section['_source']['episode_id']])
            transcript_time += time.time() - loop_start
            if section['_source']['episode_id'] not in metadatas:
                metadatas[section['_source']['episode_id']] = self.metadata_from_episode(section['_source']['episode_id'])
            metadata = metadatas[section['_source']['episode_id']]
            metadata_time += time.time() - loop_start
            res.append({
                'show': metadata['show_name'],
                'episode': metadata['episode_name'],
                'start_time': section['_source']['start_time'],
                'transcript': transcript,
                '_score': section['_score'],
            })
        
        print('transcript time:', transcript_time)
        print('metadatat time:', metadata_time)
        print('Time taken:', time.time() - start)
        return res

if __name__ == '__main__':

    searcher = Searcher()
    # s = ' backflip. We rock papr scissors'
    s = ' backflip. We rock paper scissors'
    # s = 'hey'

    sections = searcher.section_for_frontend(s, minutes=2, weighted=False)

    for section in sections[:2]:
        print('-----------------------------------------------------')
        print('Transcript:', section['transcript'])
        print('Show:', section['show'])
        print('Episode:', section['episode'])
        print('Score:', section['_score'])
        print('-----------------------------------------------------')
        print()
   
    