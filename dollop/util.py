import pandas as pd
import requests
import justext
import re

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

def get_text(urls,path):
    """
    Args:
        urls: URLs list of colombian newspaper

    """
    count_file = 1
    for url in urls:
        pdf = re.search(".pdf$",str(url))
        if pdf:
            continue
        name_file = f"{path}/text-{count_file}"
        try:
            response = requests.get(url, timeout=2.5)
            paragraphs = justext.justext(response.content, justext.get_stoplist('Spanish'))
        except Exception:
            continue
        file_name = open(name_file,"w")
        for paragraph in paragraphs:
            if not paragraph.is_boilerplate:
                file_name.write(f"{paragraph.text}\n")
        file_name.close()
        count_file += 1
