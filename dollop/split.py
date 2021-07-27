import spacy
import os
import json
from tqdm import tqdm


nlp = spacy.load("es_core_news_sm")

def split_sentences(path):
    dir_json_files = os.listdir(path)
    dict_list = []
    for file_name in dir_json_files:
        sentence_dict = {}
        with open(path + f"/{file_name}",) as f:
            data = json.loads(f.read())
        doc = nlp(data['text'])
        sentence_dict['sentences'] = list(map(str,list(doc.sents)))
        sentence_dict['url'] = data['url']
        dict_list.append(sentence_dict)
    return dict_list

def save_sentences(sentence_dict,path):
    file_number = 1
    print("Starting split into  sentences")
    for sentence in tqdm(sentence_dict):
        file_name = f"{path}/sentence_{file_number}.json"
        with open(file_name,'w') as f:
            s = json.dumps(sentence, ensure_ascii=False)
            s.encode('utf-8')
            f.write(s)
        file_number += 1



