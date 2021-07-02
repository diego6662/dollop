import luigi
import os
import pandas as pd
from task.util import get_urls, get_text 


class GetData(luigi.Task):
# First step get the data base and convert to csv

    def output(self):
        
        return luigi.LocalTarget('data/data.csv')

    def run(self):

        df = pd.read_excel('data/datos.xlsx')
        df.to_csv('data/data.csv',index=False)


class Scrape(luigi.Task):
# Second step get all url, scrape, clean the text and save in a txt file

    def requires(self):
        
        return GetData()
    
    def output(self):

        return luigi.LocalTarget("files/")

    def run(self):
        df  = pd.read_csv('data/data.csv')
        url_list = get_urls(df)
        os.mkdir("files/")
        path = os.path.abspath("files/")
        get_text(url_list,path)

