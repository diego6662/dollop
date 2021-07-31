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

def split_sentences(path):
    dir_json_files = os.listdir(path)
    dict_list = []
    print('Start split sentences!')
    for file_name in tqdm(dir_json_files):
        sentence_dict = {}
        with open(path + f"/{file_name}",) as f:
            data = json.loads(f.read())
        doc = nlp(data['text'])
        sentence_dict['sentences'] = list(map(str,list(doc.sents)))
        sentence_dict['url'] = data['url']
        dict_list.append(sentence_dict)
    return dict_list

def save_sentences(sentence_dict,path):
    file_name = path + 'ner_sentences.jsonl'
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

def ner_data(sentences):
    ner_sentences = []
    print("Start NER tagging")
    for sentence in tqdm(sentences):
        phrase_count = 1
        phrase_dict = {}
        for phrase in sentence['sentences']:
            phrase_dict[f'phrase-{phrase_count}'] = clean_transformer_output(nlp_ner(phrase.replace('\n',' ')))
            phrase_count += 1
        
        sentence['ner'] = phrase_dict
        ner_sentences.append(sentence)
    print("Finish NER tagging")
    return ner_sentences


        
