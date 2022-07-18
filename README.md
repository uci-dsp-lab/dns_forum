<div id="top"></div>

## About The Project

Multiprocessing crawlers were built to crawl data from different forums with polite frequencies. 
Each crawler consists of classes:
1. Crawler class: stores the browser headers, interacts with our database server, sends & receives data from the corresponding forum website, and handles failed requests.
2. Refiner class: called by the Crawler class to extract useful information from raw HTML contents sent by forum websites.
3. Parser class: called by the Crawler class to lemmatize posts and replies to generate tokens.

TF-IDF was generated for each dataset and then classified by using DBSCAN and K-means.


### Built With

* [NLTK][NLTK-url]
* [Beautiful Soup][BeautifulSoup-url]
* [PyMongo][PyMongo-url]
* [urllib3][urllib-url]
* [sklearn][sklearn-url]


## Data Storage Example
![MongoDB](https://github.com/uci-dsp-lab/dns_forum/blob/main/Images/mgdb_example.PNG)


## Classification Example
![Classi](https://github.com/uci-dsp-lab/dns_forum/blob/main/Images/visual_example.PNG)



[NLTK-url]: https://www.nltk.org/
[BeautifulSoup-url]: https://beautiful-soup-4.readthedocs.io/en/latest/#
[PyMongo-url]: https://pymongo.readthedocs.io/en/stable/
[urllib-url]: https://urllib3.readthedocs.io/en/stable/
[sklearn-url]: https://scikit-learn.org/stable/
