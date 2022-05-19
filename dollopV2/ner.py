from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import time
from typing import Dict, List
import spacy
import os
import json
from tqdm import tqdm
from transformers import AutoTokenizer, AutoModelForTokenClassification ,pipeline
  

tokenizer = AutoTokenizer.from_pretrained("mrm8488/bert-spanish-cased-finetuned-ner")

model = AutoModelForTokenClassification.from_pretrained("mrm8488/bert-spanish-cased-finetuned-ner")

nlp_ner = pipeline(
    "ner",
    grouped_entities = True,
    model=model,
    tokenizer=tokenizer,
    )

nlp = spacy.load("es_core_news_sm")

def _split_sentences(path: str):
    def splitter(file_name: str) -> Dict:
        sentence_dict = {}
        with open(path + f"/{file_name}",) as f:
            data = json.loads(f.read())
        doc = nlp(data['text'])
        sentence_dict['sentences'] = list(map(str,list(doc.sents)))
        sentence_dict['url'] = data['url']
        return sentence_dict
    return splitter

def split_sentences(path):
    dir_json_files = os.listdir(path)
    splitter = _split_sentences(path)
    print('Start split sentences!')
    # maybe is a good idea change thread to process
    with ThreadPoolExecutor(max_workers=8) as executor:
        dict_list = executor.map(splitter, dir_json_files)
    dict_list = filter(lambda x: x['sentences'] != [], dict_list)
        
    return dict_list

def _ner_helper(sentence: Dict) -> Dict:
    phrase_dict = {}
    for phrase_count, phrase in enumerate(sentence['sentences']):
        phrase_dict[f'phrase-{phrase_count}'] = clean_transformer_output(nlp_ner(phrase.replace('\n',' ')))
    sentence['ner'] = phrase_dict
    return phrase_dict

def ner_data(sentences):
    print("Start NER tagging")
    with ProcessPoolExecutor() as executor:
        ner_sentences = executor.map(_ner_helper,sentences)
    print("Finish NER tagging")
    return ner_sentences

def save_sentences(sentence_dict,path):
    file_name = path + 'ner_sentences.json'
    print('Start writing sentences into JSONL file!')
    with open(file_name,'w',encoding='utf-8') as f:
        for sentence in tqdm(sentence_dict):
            s = json.dumps(sentence, ensure_ascii=False)
            s.encode('utf-8')
            f.write(s + '\n')
    print('Finish writing')

def clean_transformer_output(dict_list):
    clean_list = []
    values = ('entity_group','word')
    for item in dict_list:
        new_dict = {}
        new_dict[values[0]] = item[values[0]]
        new_dict[values[1]] = item[values[1]]
        clean_list.append(new_dict)
    return clean_list