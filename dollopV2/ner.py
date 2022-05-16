from concurrent.futures import ThreadPoolExecutor
from typing import Dict
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