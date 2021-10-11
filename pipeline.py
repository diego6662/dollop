import luigi
import os
import pandas as pd
from dollop.scrape import get_urls, get_text 
from dollop.ner import split_sentences, save_sentences,ner_data
from dollop.expand import read_data, merge_entities, data_expansion, merge_samples

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
