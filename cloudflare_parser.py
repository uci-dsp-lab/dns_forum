from html.parser import HTMLParser
from nltk.tokenize import RegexpTokenizer
from nltk.stem import WordNetLemmatizer

from collections import defaultdict, deque
from nltk.corpus import stopwords

class HTMLParser_forCrawler(HTMLParser):
    def __init__(self):
        HTMLParser.__init__(self)
        self.titles = ["title", "h1", "h2", "h3", "h4", "h5", "h6"]
        self.isTitle = False
        self.words = []
        self.headers = []

    def handle_starttag(self, tag, attrs):
        if tag in self.titles:
            self.isTitle = True

    def handle_endtag(self, tag):
        if tag in self.titles:
            self.isTitle = False

    def handle_data(self, data):
        if self.isTitle:
            self.headers.append(data.strip("\n "))
        else:
            self.words.append(data.strip("\n "))

class HTML_DataHandler:
    def __init__(self):
        self.stopwords_dict  = defaultdict(int)
        for sWord in stopwords.words('english'):
            self.stopwords_dict[sWord] += 1

        self.tokenDict = defaultdict(int)
        self.tokenizer = RegexpTokenizer(r'\w+')
        self.parsed_count = 0

        self.content = ""

    def handle_htmldata(self, html_response):
        parser = HTMLParser_forCrawler()
        parser.feed(html_response)
        self.pure_lemmatize(parser.words)
        self.parsed_count += 1

        return self.content

    def pure_lemmatize(self, words):
        if not words:
            return

        for line in words:
            token_list = self.tokenizer.tokenize(line)
            if not token_list:
                continue
            for index in range(len(token_list)):
                token = token_list[index]

                self.tokenDict[token] += 1
                self.content += token
                if index != len(token_list) - 1:
                    self.content += " "

            self.content += "\n"

    def to_lower(self, token):
        upperCount = 0
        for char in token:
            if char.isupper():
                upperCount += 1
            if upperCount > 1:
                return token
        return token.lower()

