import pandas as pd
import requests
import justext


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

    Output:
        Returns a string containing all relevant text of the newspaper
    """
    for i in range(len(urls)):
        name_file = f"{path}/text-{i}"
        file_name = open(name_file,"w")
        try:
            response = requests.get(urls[i])
        except :
            continue
        paragraphs = justext.justext(response.content, justext.get_stoplist('Spanish'))
        for paragraph in paragraphs:
            if not paragraph.is_boilerplate:
                file_name.write(f"{paragraph.text}\n")
        file_name.close()
    return files
