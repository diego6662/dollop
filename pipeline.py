import luigi
import os
import pandas as pd
from dollop.scrape import get_urls, get_text 
from dollop.split import split_sentences, save_sentences


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
    sentences_folder = 'pipeline_data/sentences_data/'

    def requires(self):
        return ScrapeUrl()
    
    def output(self):
        return luigi.LocalTarget(self.sentences_folder)

    def run(self):
        scraped_data_path = os.path.abspath(self.requires().txt_folder)
        os.mkdir(self.sentences_folder)
        sentences_dict = split_sentences(scraped_data_path)
        path = os.path.abspath(self.sentences_folder)
        save_sentences(sentences_dict,path)

        
