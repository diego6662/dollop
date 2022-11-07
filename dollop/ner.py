import spacy
import os
import json
from tqdm import tqdm
from transformers import AutoTokenizer, AutoModelForTokenClassification ,pipeline
  

tokenizer = AutoTokenizer.from_pretrained("mrm8488/bert-spanish-cased-finetuned-ner",)

model = AutoModelForTokenClassification.from_pretrained("mrm8488/bert-spanish-cased-finetuned-ner",)


nlp_ner = pipeline(
    "ner",
    model="mrm8488/bert-spanish-cased-finetuned-ner",
    tokenizer=(
        'mrm8488/bert-spanish-cased-finetuned-ner',  
        {
            "use_fast": True,
        }
    ),

    aggregation_strategy = 'max'
)



nlp = spacy.load("es_core_news_lg")

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
    file_name = path + 'ner_sentences.json'
    print('Start writing sentences into JSONL file!')
    with open(file_name,'w',encoding='utf-8') as f:
        for sentence in tqdm(sentence_dict):
            s = json.dumps(sentence, ensure_ascii=False)
            s.encode('utf-8')
            f.write(s + '\n')
    print('Finish writing')

def clean_transformer_output(dict_list,document):
    output = {
            'document': document,
            'tokens': []
            }
    for index, item in enumerate(dict_list):
        new_dict = {}
        new_dict['type'] = item['entity_group']
        new_dict['text'] = item['word']
        new_dict['start'] = str(item['start'])
        new_dict['end'] = str(item['end'])
        new_dict['token_start'] = str(index)
        output['tokens'].append(new_dict)

    return output

def ner_data(sentences):
    ner_sentences = []
    print("Start NER tagging")
    for sentence in tqdm(sentences):
        phrase_count = 1
        phrase_dict = {}
        for i in range(len(sentence['sentences'])):
            try:
                # phrase_dict[f'phrase-{phrase_count}'] = nlp_ner(phrase.replace('\n',' '))
                phrase_dict[f'phrase-{phrase_count}'] = clean_transformer_output(nlp_ner(sentence['sentences'][i].replace('\n',' ')),
                        sentence['sentences'][i],)
                phrase_count += 1
            except :
                continue
        
        sentence['ner'] = phrase_dict
        ner_sentences.append(sentence)
    print("Finish NER tagging")
    return ner_sentences



