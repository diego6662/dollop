import pandas as pd
import os
from concurrent.futures import ProcessPoolExecutor
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
    entities_df = entities_df[['one', 'target', 'source1']]
    entities_df.rename(columns={'source1':'url'}, inplace=True)
    merge_df = pd.merge(entities_df,df, how='inner', on='url')
    return merge_df

def data_expansion(df):
    sample_list = []
    ner_list = df['ner'].values.tolist()
    entities_1 = df['one'].values.tolist()
    entities_2 = df['target'].values.tolist()
    for i in tqdm(range(len(ner_list))):
        phrases_list = ner_list[i].values()
        e1_found = False
        e2_found = False
        sample = False
        for ner_phrase in phrases_list:
            for phrase in ner_phrase:
                e1_found = match.valid_entity(entities_1[i], phrase['word'], phrase['entity_group']) if not e1_found else e1_found
                e2_found = match.valid_entity(entities_2[i], phrase['word'], phrase['entity_group']) if not e2_found else e2_found
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
