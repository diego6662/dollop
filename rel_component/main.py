import random
import typer
from pathlib import Path
import spacy
from spacy.tokens import DocBin, Doc
from spacy.training.example import Example
from rel_pipe import make_relation_extractor, score_relations
from rel_model import create_relation_model, create_classification_layer, create_instances, create_tensors

nlp = spacy.load("es_core_news_sm")

# text = '''Todos contra Odebrecht En el caso de la Contraloría las investigaciones sobre el proyecto Ruta del Sol II y el crédito del Banco Agrario al consorcio Navelena solo comenzaron hasta enero del presente año.'''
# text = '''Y para los investigadores no hay duda alguna de que Mestre es junto con Giraldo es el principal relacionista público del cartel de Cali con la clase política colombiana'''
text = '''El día que Fernando Botero señaló a Samper por el proceso 8.000 Hace dos décadas, Colombia empezó a vivir el momento más tenso del escándalo judicial del proceso 8.000.'''
doc = nlp(text)
# We load the relation extraction (REL) model
nlp2 = spacy.load("training/model-best")
# We take the entities generated from the NER pipeline and input them to the REL pipeline
for name, proc in nlp2.pipeline:
    doc = proc(doc)
# Here, we split the paragraph into sentences and apply the relation extraction for each pair of entities found in each sentence.
def relation(d: dict):
    r = None
    s = float('-inf')
    for k,v in d.items():
        if v > s:
            r = k
            s = v
    return r


for value, rel_dict in doc._.rel.items():
    for sent in doc.sents:
        for e in sent.ents:
            for b in sent.ents:
                if e.start == value[0] and b.start == value[1]:
                    print(f" entities: {e.text, b.text} --> predicted relation: {rel_dict}")

