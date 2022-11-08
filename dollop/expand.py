import pandas as pd
import os
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from tqdm import tqdm
from .matcher import Match

match = Match()
match.clean_people()
match.clean_organization()

def read_data(path):
    files_dir = os.listdir(path)
    path = path + files_dir[0]
    df = pd.read_json(path, lines=True, encoding='utf-8')
    return df

def merge_entities(df):
    entities_df = pd.read_csv('pipeline_data/csv_data/links.csv')
    entities_df = entities_df[['one', 'target', 'source1', 'rel_type']]
    entities_df.rename(columns={'source1':'url'}, inplace=True)
    merge_df = pd.merge(entities_df,df, how='inner', on='url')
    return merge_df

def data_expansion(df):
    sample_list = []
    ner_list = df['ner'] 
    entities_1 = df['one'].values.tolist()
    entities_2 = df['target'].values.tolist()
    for i in tqdm(range(len(ner_list))):
        phrases_list = ner_list[i]
        e1_found = False
        e2_found = False
        sample = False
        for ner_phrase in phrases_list:
            for phrase in ner_phrase['tokens']:
                e1_found = match.valid_entity(entities_1[i], phrase['text'], phrase['entityLabel']) if not e1_found else e1_found
                e2_found = match.valid_entity(entities_2[i], phrase['text'], phrase['entityLabel']) if not e2_found else e2_found
                if e1_found and e2_found:
                    sample = True
                    break
            if sample:
                break
        sample_list.append(sample)
    return sample_list
                
def merge_samples(df,samples):
    df['sample'] = samples
    return df

def _add_entities(batch):
    entity_1 = batch[0]
    entity_2 = batch[1]
    rel = batch[2]
    docs = batch[3]
    result = []
    for doc in docs:
        relations = []
        tokens = doc.get('tokens')
        n_tokens = len(tokens)
        if n_tokens > 1:
            for i in range(n_tokens):
                e1_found = match.valid_entity(entity_1, tokens[i]['text'], tokens[i]['entityLabel'])
                e2_found = match.valid_entity(entity_2, tokens[i]['text'], tokens[i]['entityLabel'])
                found_one = e1_found or e2_found
                if found_one:
                    for j in range(i + 1, n_tokens):
                        if e2_found:
                            e1_found = match.valid_entity(
                                entity_1,
                                tokens[j]['text'], 
                                tokens[j]['entityLabel'])
                        else:
                            e2_found = match.valid_entity(
                                entity_2, 
                                tokens[j]['text'], 
                                tokens[j]['entityLabel'])
                        if e1_found and e2_found:
                            child = tokens[j]['token_start']
                            head = tokens[i]['token_start']
                            if head < child:
                                relation = {
                                'child': child,
                                'head': head,
                                'relationLabel': rel
                                }
                            else:
                                relation = {
                                'child': head,
                                'head': child,
                                'relationLabel': rel
                                }
                            if not relation in relations:
                                relations.append(relation)
        doc['relations'] = relations
        result.append(doc)
    return result




    

def add_relationship(df):
    entities_1 = df['one'].values.tolist()
    entities_2 = df['target'].values.tolist()
    rel_types = df['rel_type'].values.tolist()
    ner_sentences = df['ner'].values.tolist()
    batch = zip(entities_1, entities_2, rel_types, ner_sentences)
    with ProcessPoolExecutor() as executor:
        result = executor.map(_add_entities, batch)
    result = [doc for l in result for doc in l]
    result = filter(lambda x: x['relations'] != [], result)
    rel_types = set(rel_types)
    rel_types = {r:r for r in rel_types}
    return [*result], rel_types