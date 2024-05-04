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
        self.pointer += n - 1
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
                'num_hits': len(sections),
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
            size = 100,
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
    
    def concatenate_with_sections(self, section_id, episode_id, n_minutes):
        section_id = int(section_id)
        sections = self.get_section_span(section_id, episode_id, n_minutes)

        section_index = self.index_of_section(section_id, sections) 
        if section_index < 0:
            raise ValueError('Section not found in sections')
        
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

        for section in sections[start_index:end_index]:
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

    def do_search(self, query, minutes, weighted=False):
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

    def section_for_frontend(self, query, minutes, weighted=False):
        start = time.time()
        sections = []

        if weighted:
            episode_id_score = self.episodes_from_query(query)
            #sections = self.sections_from_episodes(episode_id_score, query)
            sections = self.sections_from_episodes_query(episode_id_score, query)

            sections = self.rank_sections_weighted(sections, episode_id_score)
        else:
            sections = self.sections_from_query(query)
            sections = self.rank_sections_only(sections)
            """
            episode_id_score = self.episodes_from_query(query)
            sections, sections_map = self.sections_from_episodes(episode_id_score, query)
            sections = self.rank_sections_only(sections)
            """


        before_stuff_time = time.time() - start
        print('before stuff time ', before_stuff_time)

        res = []
        transcript_time = 0
        metadata_time = 0
        
        metadatas = {}
        for section in sections:
            transcript_time_start = time.time()
            # query to get previus and next sections to get entire span of transcripts
            transcript = self.get_section_span(int(section['_id']), section['_source']['episode_id'], minutes)
            transcript_time += time.time() - transcript_time_start

            loop_start = time.time()
            # this is to speedup the process of getting metadata
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

    start = time.time()

    searcher.do_search(s, 2, weighted=True)
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
    
