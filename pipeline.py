import luigi
import os
import pandas as pd
from dollop.scrape import get_urls, get_text 


class WhoIsToCsv(luigi.Task):
# First step get the data base and convert to csv
    path = 'pipeline_data/csv_data/data.csv'

    def output(self):
        return luigi.LocalTarget(self.path)

    def run(self):
        df = pd.read_excel('pipeline_data/csv_data/datos.xlsx')
        df.to_csv(self.path, index=False)


class ScrapeUrl(luigi.Task):
# Second step get all url, scrape, clean the text and save in a txt file
    txt_folder = 'pipeline_data/scraped_data/'
    
    def requires(self):
        return WhoIsToCsv()
    
    def output(self):
        return luigi.LocalTarget(self.txt_folder)

    def run(self):
        df  = pd.read_csv(self.requires().path)
        url_list = get_urls(df)
        os.mkdir(self.txt_folder)
        path = os.path.abspath(self.txt_folder)
        get_text(url_list, path)

