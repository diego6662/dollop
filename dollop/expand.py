import pandas as pd
import os
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
    entities_df.rename(
            columns={'source1':'url', 'one':'entity-1', 'target':'entity-2','rel_type':'relation'},
                    inplace=True
                    )
    merge_df = pd.merge(entities_df,df, how='inner', on='url')
    return merge_df

def data_expansion(df):
    sample_list = []
    ner_list = df['ner'].values.tolist()
    entities_1 = df['entity-1'].values.tolist()
    entities_2 = df['entity-2'].values.tolist()
    for i in tqdm(range(len(ner_list))):
        phrases_list = ner_list[i].values()
        e1_found = False
        e2_found = False
        sample = False
        for ner_phrase in phrases_list:

            ner_phrase = ner_phrase['tokens']
            for phrase in ner_phrase:
                e1_found = match.valid_entity(entities_1[i], phrase['text'], phrase['type']) if not e1_found else e1_found
                e2_found = match.valid_entity(entities_2[i], phrase['text'], phrase['type']) if not e2_found else e2_found
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

def create_relationships(entity1,entity2,relation,tokens):
    # e1_posible = match.search_org(entity1)
    # e2_posible = match.search_org(entity2)
    token_list = tokens['tokens']
    tokens['relations'] = []
    for idx,token in enumerate(token_list):
        equal_to_e1 = match.valid_entity(entity1,token['text'],token['type'])
        equal_to_e2 = match.valid_entity(entity2,token['text'],token['type'])
        if equal_to_e1:
            for idx2, token2 in enumerate(token_list[idx + 1:]):
                entity_aux = match.valid_entity(entity2,token2['text'],token2['type'])
                if entity_aux == False:
                    continue
                relations = dict()
                relations['child'] = token2['token_start']
                relations['head'] = token['token_start']
                relations['relation_label'] = relation
                tokens['relations'].append(relations)
        elif equal_to_e2:
            for idx2, token2 in enumerate(token_list[idx + 1:]):
                entity_aux = match.valid_entity(entity1,token2['text'],token2['type'])
                if entity_aux == False:
                    continue
                relations = dict()
                relations['child'] = token2['token_start']
                relations['head'] = token['token_start']
                relations['relation_label'] = relation
                tokens['relations'].append(relations)

    if tokens['relations'] == []:
        return None
    else:
        return tokens
