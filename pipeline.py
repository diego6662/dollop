import luigi
from tqdm import tqdm
import os
import pandas as pd
from dollop.scrape import get_urls, get_text 
from dollop.ner import split_sentences, save_sentences,ner_data
from dollop.expand import read_data, merge_entities, data_expansion, merge_samples, create_relationships
from dollop.spacy_converter import extract_relations, convert_to_spacy


class WhoIsToCsv(luigi.Task):
# First step get the data base and convert to csv
    path = 'pipeline_data/csv_data/'
    files = ['links','people','officepositions','organizations','rel_types','clans']
    task_complete = False

    def requires(self):

        requires_list = [SheetToCsv(path_file = self.path, name_file = file) for file in self.files]
        return requires_list

    def run(self):
        print("csv conversion successfully")
        self.task_complete = True

    def complete(self):
        return self.task_complete


class SheetToCsv(luigi.Task):
# Convert any sheet of excel file to csv file
    path_file = luigi.Parameter()
    name_file = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(f"{self.path_file}{self.name_file}.csv")

    def run(self):
        df = pd.read_excel('pipeline_data/csv_data/datos.xlsx', sheet_name=self.name_file)
        path = f"{self.path_file}{self.name_file}.csv"
        df.to_csv(path, index=False)


class ScrapeUrl(luigi.Task):
# Second step get all url, scrape, clean the text and save in a txt file
    txt_folder = 'pipeline_data/scraped_data/'
    
    def requires(self):
        return WhoIsToCsv()
    
    def output(self):
        return luigi.LocalTarget(self.txt_folder)

    def run(self):
        df  = pd.read_csv(f"{self.requires().path}links.csv")
        url_list = get_urls(df)
        os.mkdir(self.txt_folder)
        path = os.path.abspath(self.txt_folder)
        get_text(url_list, path)


class SplitSentences(luigi.Task):
    task_complete = False
    sentences = []

    def requires(self):
        return ScrapeUrl()
    
    def output(self):
        return self.sentences

    def complete(self):
        return self.task_complete

    def run(self):
        scraped_data_path = os.path.abspath(self.requires().txt_folder)
        sentences_dict = split_sentences(scraped_data_path)
        self.sentences = sentences_dict
        self.task_complete = True
        print("Split Complete!")

        
class ExtractEntities(luigi.Task):
    sentences_folder = 'pipeline_data/ner_data/'

    def requires(self):
        return SplitSentences()

    def output(self):
        return luigi.LocalTarget(self.sentences_folder)

    def run(self):
        sentences = self.requires().output()
        os.mkdir(self.sentences_folder)
        ner_sentences = ner_data(sentences)
        save_sentences(ner_sentences,self.sentences_folder) 


class AddEntities(luigi.Task):
    task_complete = False
    expanded_json = None

    def requires(self):
        return ExtractEntities()
    
    def output(self):
       return self.expanded_json
    
    def complete(self):
        return self.task_complete

    def run(self):
        print("Merging whois entities")
        data = read_data(self.requires().sentences_folder)
        merge_data = merge_entities(data)
        self.expanded_json = merge_data
        self.task_complete = True
        print("Done")


class ExpandData(luigi.Task):
    expand_folder = 'pipeline_data/expanded_data/'
 
    def requires(self):
        return AddEntities()

    def output(self):
        return luigi.LocalTarget(self.expand_folder)

    def run(self):
        print("Start to expanding dataframe")
        df = self.requires().output()
        df.drop(df[df['ner'] == {}].index, inplace=True)
        sample_list = data_expansion(df)
        df = merge_samples(df,sample_list)
        os.mkdir(self.expand_folder)
        df.to_csv('test.csv',index=False)
        path = self.expand_folder + 'ExpandData.json'
        df.to_json(path, lines=True,  orient='records', force_ascii=False )
        print("Done")


class RemoveNegativeSamples(luigi.Task):
    task_complete = False
    clean_data = None
    def requires(self):
        return ExpandData()

    def output(self):
        return self.clean_data

    def complete(self):
        return self.task_complete

    def run(self):
        folder_data = self.requires().expand_folder
        data_json = folder_data  + 'ExpandData.json'
        data_json = pd.read_json(data_json, lines=True)
        data_json.drop(data_json[data_json['sample'] == False].index, inplace=True)
        self.clean_data = data_json
        self.task_complete = True


class CreateRelationship(luigi.Task):
    data_folder = 'pipeline_data/relationship/'
    train_data = None

    def requires(self):
        return RemoveNegativeSamples()

    def output(self):
        return self.train_data

    def complete(self):
        path = self.data_folder + 'train_data.json'
        if os.path.exists(path):
            self.train_data = pd.read_json(path, lines=True)
            return True
        else:
            return False

    def run(self):
        data = self.requires().output()
        # entity-1, entity-2, relation, ner
        entity_1 = data['entity-1']
        entity_2 = data['entity-2']
        relations = data['relation']
        ner = data['ner']
        data_to_train = []
        print('start creation of train data')
        for i in tqdm(range(entity_1.shape[0])):
            tokens = ner.iloc[i]  # this dict contain phrase-1 to phrase-n
            # and every phrase have the following keys type, text, start, end, token_start
            # I need create the relationship that have this relationship
            for j in tokens.keys():
                rel = create_relationships(entity1 = entity_1.iloc[i], entity2= entity_2.iloc[i],
                                     relation=relations.iloc[i],tokens=tokens[j])
                if rel == None:
                    continue
                else:
                    data_to_train.append(rel)
        df_output = pd.DataFrame(data_to_train)
        os.mkdir(self.data_folder)
        path = self.data_folder + 'train_data.json'
        df_output.to_json(path, lines=True,  orient='records', force_ascii=False )
        self.train_data = df_output


class ConvertToSpacy(luigi.Task):
    train_folder = 'pipeline_data/train_data/'
    spacy_data_loc = train_folder + 'train.spacy'

    def requires(self):
        return CreateRelationship()

    def output(self):
        return luigi.LocalTarget(self.spacy_data_loc)

    def run(self):
        print('Start conversion into spacy format')
        df = self.requires().output()
        relations = extract_relations()
        os.mkdir(self.train_folder)
        convert_to_spacy(relations, df, self.spacy_data_loc)
        print('Finish conversion into spacy format')
