import pandas as pd
import requests
import justext
import re
from tqdm import tqdm
from .logging import Logger 
import json


def get_urls(df):
    """
    Args:
        df: A pandas DataFrame

    Output:
        Returns a list of URLs
    """ 
    url_fields = ['source1', 'source2', 'source3']
    dfs = [ df[[col]].rename(columns={col:'url'}) for col in url_fields ]
    urls_df = pd.concat(dfs)
    return urls_df.url.unique().tolist()

def get_text(urls, path):
    """
    Args:
        urls: URLs list of colombian newspaper

    """
    count_file = 1
    logger = Logger(urls)
    for url in tqdm(urls[:50]):
        pdf = re.search(".pdf$",str(url))
        if pdf:
            logger.pdf_error()
            continue
        name_file = f"{path}/text-{count_file}.json"
        try:
            response = requests.get(url, timeout=2.5)
            paragraphs = justext.justext(response.content, justext.get_stoplist('Spanish'))
        except Exception:
            logger.generic_error()
            continue
        data = {'url':url}
        text = ""
        for paragraph in paragraphs:
            if not paragraph.is_boilerplate:
                text += f"{paragraph.text}\n"
        data['text'] = text

        with open(name_file,"w") as f:
            json.dump(data,f)

        count_file += 1
    logger.close(count_file - 1)
