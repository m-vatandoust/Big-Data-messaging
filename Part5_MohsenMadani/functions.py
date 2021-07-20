from hazm import *
from sklearn.feature_extraction.text import TfidfVectorizer
import pandas as pd

def FindKeyWords(stop_words , syms, normText):
    txt_LIST = list()
    txt_LIST.append(normText) 
    out = []
    
    words = normText.split()
    filter_words = [w for w in words if not w in stop_words]

    # identify spesific words:
    for i in syms:
        if i in words:
            out.append(i)

    # stemmer = Stemmer()
    # stem_words = [stemmer.stem(w) for w in filter_words]

    filtered_sentence = (" ").join(filter_words)
    
    # Calculate tf-idf for extracting keyWords:
    tfIdfVectorizer=TfidfVectorizer(use_idf=True)
    tfIdf = tfIdfVectorizer.fit_transform( [filtered_sentence] )
    df = pd.DataFrame(tfIdf[0].T.todense(), index=tfIdfVectorizer.get_feature_names(), columns=["TF-IDF"])
    df = df.sort_values('TF-IDF', ascending=False)
    keyWord = df.index[0]

    out.append(keyWord)

    return out 

