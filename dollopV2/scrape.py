from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional
import pandas as pd
import requests
import justext
import re
from tqdm import tqdm
import json
import logging 


def get_urls(df: pd.DataFrame) -> List[str]:
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

def _get_text():
    """
    closure to paralellize the text scraping 
    """
    def scrape_text(url: str) -> Optional[Dict]:
        """
        function to scrape text from an url
        Args:
            url: String with the url of a newspaper
        Output:
            Return a dictionary with the url of the new as key and the text as value.
        """

        pdf = re.search(".pdf$",str(url))
        if pdf:
            return
        try:
            response = requests.get(url, timeout=2)
            # paragraphs = justext.justext(response.content, justext.get_stoplist('Spanish'), encoding='iso-8859-1')
            paragraphs = justext.justext(response.content, justext.get_stoplist('Spanish'), encoding='utf-8')
        except Exception:
            return
        data = {'url':url}
        text = ""
        for paragraph in paragraphs:
            if not paragraph.is_boilerplate:
                text += f"{paragraph.text}\n"
        data['text'] = text
        return data 
    return scrape_text

def get_text(urls:str, path:str):
    """
    Args:
        urls: URLs list of newspapers
        path: path of the folder to save the text objects

    """
    print("Start scraping text")
    scrape_function = _get_text()
    with ThreadPoolExecutor() as executor:
        scraped_text = executor.map(scrape_function,urls)
    scraped_text = filter(lambda x: x is not None, scraped_text)
    for i, data in tqdm(enumerate(scraped_text)):
        name_file = f"{path}/text-{i}.json"
        with open(name_file,"w") as f:
            s = json.dumps(data, ensure_ascii=False)
            s.encode('utf-8')
            f.write(s)

