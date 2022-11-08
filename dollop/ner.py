from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from hashlib import new
from re import I
from typing import Dict, List
import spacy
import os
import json
from tqdm import tqdm
from transformers import AutoTokenizer, AutoModelForTokenClassification ,pipeline
from spacy import tokenizer
from spacy.lang.es import Spanish
nlp_2 = Spanish()
tkz = tokenizer.Tokenizer(nlp_2.vocab)
# from flair.data import Sentence
# from flair.models import SequenceTagger

# load tagger
# tagger = SequenceTagger.load("flair/ner-spanish-large")

# make example sentence

tokenizer = AutoTokenizer.from_pretrained("mrm8488/bert-spanish-cased-finetuned-ner",max_length=700, padding=True, truncation=True, add_special_tokens = True)

model = AutoModelForTokenClassification.from_pretrained("mrm8488/bert-spanish-cased-finetuned-ner")

nlp_ner = pipeline(
    "ner",
    aggregation_strategy="max",
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
    with ThreadPoolExecutor() as executor:
        dict_list = executor.map(splitter, dir_json_files)
    dict_list = filter(lambda x: x['sentences'] != [], dict_list)
        
    return [*dict_list]

def _ner_helper(sentence: Dict) -> Dict:
    ner_list = []
    for phrase_count, phrase in enumerate(sentence['sentences']):
        try:
            result = {'document': phrase}
            phrase_clean = phrase.replace('\n',' ')
            phrase_list = clean_transformer_output(
                nlp_ner(
                    phrase_clean
                ),
                phrase_clean
            )
            if phrase_list:
                result['tokens'] = phrase_list
                ner_list.append(result)
            else:
                continue
        except:
            continue
    sentence['ner'] = ner_list
    return sentence

def ner_data(sentences):
    print("Start NER tagging")
    with ProcessPoolExecutor() as executor:
        ner_sentences = executor.map(_ner_helper,sentences)
    print("Finish NER tagging")
    return ner_sentences

def save_sentences(sentence_dict,path):
    file_name = path + 'ner_sentences.jsonl'
    print('Start writing sentences into JSONL file!')
    with open(file_name,'w',encoding='utf-8') as f:
        for sentence in tqdm(sentence_dict):
            s = json.dumps(sentence, ensure_ascii=False)
            s.encode('utf-8')
            f.write(s + '\n')
    print('Finish writing')

def clean_transformer_output(dict_list, phrase):
    clean_list = []
    for item in dict_list:
        new_dict = {}
        new_dict['entityLabel'] = item['entity_group']
        new_dict['text'] = item['word']
        new_dict['start'] = item['start']
        new_dict['end'] = item['end']
        word_tokens = item['word'].split(' ')
        word_tokens = [word.text for word in tkz(item['word'])]
        tokens = [word.text for word in tkz(phrase)]
        index = find_sub_list(word_tokens, tokens)
        index = index if index else (None, None)
        new_dict['token_start'] = index[0]
        new_dict['token_end'] = index[1]
        clean_list.append(new_dict)
    clean_list = filter(lambda x: x['token_start'] is not None, clean_list)
    return [*clean_list]

def find_sub_list(sl,l):
    sll=len(sl)
    for ind in (i for i,e in enumerate(l) if e==sl[0]):
        if l[ind:ind+sll]==sl:
            return ind,ind+sll-1