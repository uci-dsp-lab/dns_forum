"""
Author: c0ldstudy
2022-04-08 10:55:19
"""

import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer

import pandas as pd
import numpy as np
from collections import defaultdict
import random
import pymongo
from sklearn.feature_extraction.text import TfidfTransformer, TfidfVectorizer, CountVectorizer
from sklearn.metrics import pairwise_distances
from scipy.spatial.distance import cosine

import numpy as np
from sklearn.cluster import DBSCAN
from sklearn import metrics
from sklearn.datasets import make_blobs
from sklearn.preprocessing import StandardScaler

def lemma(word, lemmatizer):
    word = word.strip()
    safe = ["ns"]
    if word in safe:
        return word
    noun = lemmatizer.lemmatize(word)
    if noun != word:
        return noun
    adjective = lemmatizer.lemmatize(word, pos="a")
    if adjective != word:
        return adjective
    verb = lemmatizer.lemmatize(word, pos="v")
    if verb != word:
        return verb
    return word


def data_process():
    # nltk.download('stopwords')
    # nltk.download('wordnet')
    # nltk.download('omw-1.4')
    # updata the stopwords
    lemmatizer = WordNetLemmatizer()
    stopwordDict = defaultdict(int)
    for sw in stopwords.words("english"):
        stopwordDict[sw] += 1

    # data loader
    client = pymongo.MongoClient(host="128.195.180.83", port=27939, username="db_viewer", password="ucidsplab_dbviewer")
    db = client.cloudflare_crawled_data
    vocab = {}
    corpus, corpus_index, url_list = [], [], []
    startPage, endPage = 100, 1354

    for i in range(startPage, endPage + 1):
        col_name = "purepage" + str(i)
        collection = db[col_name]
        for page in collection.find():
            if not page["DNS_Related"]:
                continue
            if "Other Languages" in page["labels"]:
                continue

            unclassified = True
            if unclassified:
                unprocessed, processed = page["original_post"], []
                for word in unprocessed.split(" "):
                    lem = lemma(word.strip().lower(), lemmatizer)
                    try:
                        _ = int(lem)
                        continue
                    except:
                        pass
                    if lem not in stopwordDict:
                        flag = False
                        for char in lem:
                            if char < "a" or char > "z":
                                flag = True
                        if not flag:
                            processed.append(lem)
                            vocab[lem] = 1
                corpus.append(" ".join(processed))
                corpus_index.append(page["title"])
                url_list.append(page["url"])

    print("corpus shape: ",len(corpus))
    return corpus, vocab

def data_generator(vocab, corpus):
    vectorizer = TfidfVectorizer(min_df = 3, max_features = 10000)


# # vectorizer = TfidfVectorizer()
    X = vectorizer.fit_transform(corpus)
    tfidf_array = X
    distance_array = pairwise_distances(X, metric='cosine')



    # vocabList = vocab.keys()
    # cv1 = CountVectorizer(vocabulary=vocabList, analyzer = 'word')
    # corpus_vocab_count_matrix = cv1.transform(corpus)
    # tfidf_transformer = TfidfTransformer(smooth_idf=True, use_idf=True)
    # tfidf_transformer.fit(corpus_vocab_count_matrix)
    # df_idf = pd.DataFrame(tfidf_transformer.idf_, index=cv1.get_feature_names_out(), columns=   ['idf-weights'])
    # df_idf = df_idf.sort_values(by=['idf-weights'])

    # articles_vocab_count_matrix = cv1.transform(corpus)
    # tfidf_matrix = tfidf_transformer.transform(articles_vocab_count_matrix)
    # tfidf_array = tfidf_matrix.toarray()

    # df = pd.DataFrame(tfidf_array[0], index=cv1.get_feature_names_out(), columns=['tfidf'])
    # df_descending = df.sort_values(by=['tfidf'], ascending=False)
    # # print(df_descending.head(10))

    # distance_array = pairwise_distances(tfidf_array, metric='cosine')
    # print("distance_array: ", distance_array.shape, distance_array)
    # exit()
    return distance_array, tfidf_array

def model_training(distance_array, tfidf_array):
    for eps in np.arange(1, 5, 1):
        for min_simple in range(2,3,1):
            try:
                print(f"eps: {eps}, min_simple: {min_simple}")

                clustering = DBSCAN(eps=eps, min_samples=min_simple).fit(distance_array)
                core_samples_mask = np.zeros_like(clustering.labels_, dtype=bool)
                core_samples_mask[clustering.core_sample_indices_] = True
                labels = clustering.labels_
                score = metrics.silhouette_score(distance_array, labels)
                print(f"silhouette_score:{score}")
                print(np.unique(labels, return_counts=True))
            except Exception as e:
                print(e)



if __name__ == '__main__':
    corpus, vocab = data_process()
    distance_array, tfidf_array = data_generator(vocab, corpus)
    model_training(distance_array, tfidf_array)

