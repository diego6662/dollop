import json
from spacy.tokens import Span, DocBin, Doc
from spacy.vocab import Vocab
from spacy.tokenizer import Tokenizer
from spacy.lang.es import Spanish
from spacy.util import compile_infix_regex
import re
import spacy
import pandas as pd


def extract_relations():
    df = pd.read_json('pipeline_data/expanded_data/ExpandData.json', lines=True)
    df_relation = df["relation"]
    relations = df_relation.unique().tolist()
    relation_dict = {x:x for x in relations}
    return relation_dict 

def convert_to_spacy(map_labels, data, save_loc):
    nlp = spacy.blank("es")
    # empty tokenizer with only spanish vocabulary
    MAP_LABELS = map_labels
    # print(MAP_LABELS)
    Doc.set_extension('rel', default={}, force=True)
    vocab = Vocab()
    docs = {"train": [], "dev": [], "test": [], "total": []}
    count_all = {"train": 0, "dev": 0, "test": 0, "total": 0}
    count_pos = {"train": 0, "dev": 0, "test": 0, "total": 0}

    file_data = data 
    rows = file_data.shape[0]
    for i in range(rows):
        example = file_data.iloc[i,:]
        span_starts = set()
        neg = 0
        pos = 0
        # parse the tokens

        tokens = nlp(example["document"])
        spaces = [True if tok.whitespace_ else False for tok in tokens]
        words = [t.text for t in tokens]
        doc = Doc(nlp.vocab, words=words, spaces=spaces)

        # Parse the ggp entities
        spans = example["tokens"]
        entities = []
        span_end_to_start = {}
        for span in spans:
            entity = doc.char_span(
                    int(span["start"]), int(span["end"]), label=span["type"]
                    )
            span_end_to_start[span["token_start"]] = span["token_start"]
            entities.append(entity)
            span_starts.add(span["token_start"])
        try:
            doc.ents = entities
        except :
            continue

        rels = {}
        for x1 in span_starts:
            for x2 in span_starts:
                rels[(x1,x2)] = {}
        relations = example["relations"]
        # print(relations)
        # input()
        for relation in relations:
            start = span_end_to_start[relation["head"]]
            end = span_end_to_start[relation["child"]]
            label = relation["relation_label"]
            if label not in rels[(start,end)]:
                rels[(start,end)][label] = 1.0
                pos += 1

        for x1 in span_starts:
            for x2 in span_starts:
                for label in MAP_LABELS.values():
                    if label not in rels[(x1,x2)]:
                        neg += 1
                        rels[(x1,x2)][label] = 0.0
        doc._.rel = rels
        # print(doc._.rel)
        # input()
        # print(pos)
        # print(i)
        if pos > 0:
            docs["total"].append(doc)
            count_pos["total"] += pos
            count_all["total"] += pos + neg


    docbin = DocBin(docs = docs["total"], store_user_data=True)
    docbin.to_disk(save_loc)


