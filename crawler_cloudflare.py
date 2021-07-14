# imports for the crawler
import requests as r
import os
import time
import json
import multiprocessing
from multiprocessing import Pool

from collections import deque, defaultdict
import random

# imports for the parser
from bs4 import BeautifulSoup
from urllib import parse

# imports for mongodb
import pymongo

# The Crawler_cloudflare() class has functions:
#  - start_crawling(current_url): After initializing a class instance, call this function to start crawling the input
# cloudflare json url.
#  - retry(): It is called at the end of the start_crawling() function to retry all previously failed URLs.
#  - retry_url(url_retry, isExternal): It is responsible for the main retrying process.
#  - handle_url(current_url, isExternal): Retrieving contents from the given URL.
#  - store_crawled_data(url_response, id, slug, real_url): Storing the refined crawled data on Mongodb. After storing,
# it crawls the URLs that are included in the post contents and stores the crawled data on Mongodb.
#  - extract_next_urls(json_url): Taking in one cloudflare community page URL which contains URL information of all the
#  posts on that page in JSON format and returning those URLs.
#  - external_crawl(included_url): Crawling URLs that are included in the posts.
#  - retry_external(): Retrying failed external URLs.
#  - store_external(url_response, included_url): Storing the crawled data on Mongodb.

class Crawler_cloudflare:
    def __init__(self):
        #self.url_queue = deque([])
        #self.dir = "D:/Crawler_Project/Crawled_Data"
        self.pid = 0
        #self.doc_count = 0
        self.crawled_count = 0

        self.crawl_interval = 0.35
        self.header = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                       "Accept-Language": "en-US;q=0.9,en;q=0.8"}
        self.json_header = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                            "Accept": "application/json"}

        # These proxies are found from the Internet. I will delete them if they are not appropriate.
        # This version is temporarily NOT using proxies.

        # self.proxies = [#{"http": "174.81.78.64:48678"},
        #                 # {"http": "95.208.208.227:3128"},
        #                 # {"http": "153.120.137.91:8080"},
        #                 # {"http": "183.47.138.80:8888"},
        #                 # {"http": "120.34.216.47:9999"},
        #                 # {"http": "125.78.226.217:8888"},
        #                 # {"http": "27.150.87.5:9999"}]]

        self.failed_queue = deque([])
        self.external_failed_queue = deque([])
        self.failed_dict = defaultdict(int)
        self.external_failed_dict = defaultdict(int)

        self.json_page_num = ""
        self.collection = None

        self.already_met = defaultdict(int)

    def start_crawling(self, current_url):
        self.pid = os.getpid()
        print("PID {2} | Crawling URL: {0} Crawled: {1}".format(current_url, self.crawled_count, self.pid))
        self.crawled_count += 1

        index = -1
        while current_url[index] != "=":
            self.json_page_num = current_url[index] + self.json_page_num
            index -= 1

        client = pymongo.MongoClient(host="localhost", port=27017)
        db = client.cloudflare_crawled_data
        collection_name = "page" + self.json_page_num
        self.collection = db[collection_name]

        url_queue = self.extract_next_urls(current_url)
        for url_info in url_queue:

            real_url, id, slug = url_info[0], url_info[1], url_info[2]

            print("PID {2} | Crawling URL: {0} Crawled: {1}".format(real_url, self.crawled_count, self.pid))
            url_response = self.handle_url(real_url, 0)
            time.sleep(self.crawl_interval)

            if not url_response:
                continue

            self.store_crawled_data(url_response, id, slug, real_url)

        self.retry()

    def get_slug_id(self, url_retry):
        id, slug, index, slashCount = "", "", -1, 0
        while slashCount != 2:
            if slashCount == 0 and url_retry[index] != "/":
                id = url_retry[index] + id
            if slashCount == 1 and url_retry[index] != "/":
                slug = url_retry[index] + slug
            if url_retry[index] == "/":
                slashCount += 1
            index -= 1

        return id, slug

    def retry(self):
        while self.failed_queue:
            url_retry = self.failed_queue.popleft()
            id, slug = self.get_slug_id(url_retry)

            print("PID {2} | Retrying URL: {0} Retried times: {1}".format(url_retry, self.failed_dict[url_retry], self.pid))
            self.failed_dict[url_retry] += 1
            url_response = self.retry_url(url_retry, 0)
            time.sleep(self.crawl_interval)

            if not url_response:
                continue

            self.store_crawled_data(url_response, id, slug, url_retry)

    def retry_url_helper(self, url_retry, isExternal):
        if isExternal == 0:
            if self.failed_dict[url_retry] < 3:
                self.failed_queue.append(url_retry)
            else:
                print("PID {1} | Retry for URL: {0} FAILED.".format(url_retry, self.pid))
        else:
            if self.external_failed_dict[url_retry] < 3:
                self.external_failed_queue.append(url_retry)
            else:
                print("PID {1} | Retry for External URL: {0} FAILED.".format(url_retry, self.pid))
        return ""

    def retry_url(self, url_retry, isExternal):
        # rint = random.randint(0, len(self.proxies) - 1)
        # proxy = self.proxies[rint]
        try:
            url_response = r.get(url_retry, timeout=20, headers=self.header)
            self.crawled_count += 1
            print("PID {1} | Retry for URL: {0} SUCCESS.".format(url_retry, self.pid))
            return url_response
        except r.exceptions.RequestException as e:
            print("PID {2} | Failed to Retry: {0}, Error Message: {1}".format(url_retry, "Timeout", self.pid))
            self.retry_url_helper(url_retry, isExternal)
        except:
            print("PID {2} | Failed to Retry: {0}, Error Message: {1}".format(url_retry, r.exceptions.ConnectionError.args, self.pid))
            self.retry_url_helper(url_retry, isExternal)

    def handle_url_helper(self, current_url, isExternal):
        if isExternal == 0:
            self.failed_queue.append(current_url)
        else:
            self.external_failed_queue.append(current_url)
        return ""

    def handle_url(self, current_url, isExternal):
        # rint = random.randint(0, len(self.proxies) - 1)
        # proxy = self.proxies[rint]
        try:
            url_response = r.get(current_url, timeout=15, headers=self.header)
            self.crawled_count += 1
            return url_response
        except r.exceptions.RequestException as e:
            print("PID {2} | Failed to Crawl: {0}, Error Message: {1}".format(current_url, "Timeout", self.pid))
            self.handle_url_helper(current_url, isExternal)
        except:
            print("PID {2} | Failed to Crawl: {0}, Error Message: {1}".format(current_url, r.exceptions.ConnectionError.args, self.pid))
            self.handle_url_helper(current_url, isExternal)

    def store_crawled_data(self, url_response, id, slug, real_url):
        # The commented code was used for storing data on the disk files

        # if self.doc_count == 0 and "Page" + self.json_page_num not in os.listdir(self.dir):
        #     os.makedirs(self.dir + "/Page" + self.json_page_num)

        # current_dir = self.dir + "/Page" + self.json_page_num + "/" + slug + str(id) + ".txt"
        # file = open(current_dir, "w", encoding="utf-8")
        # file.write(url_response.text)
        # file.close()

        # self.doc_count += 1
        # if self.doc_count == 30:
        #     self.doc_count = 0

        refiner = refiner_cloudflare(url_response, real_url)
        refiner.refine()

        insert_to_db = {"title": slug + str(id),
                        "url": real_url,
                        "content": refiner.post_content,
                        "included_url": refiner.included_url}
        self.collection.insert_one(insert_to_db)

        if refiner.included_url:
            for external_url in refiner.included_url:
                self.external_crawl(external_url)

        self.retry_external()

    def extract_next_urls(self, json_url):
        # rint = random.randint(0, len(self.proxies) - 1)
        # proxy = self.proxies[rint]
        try:
            url_response = r.get(json_url, timeout=15, headers=self.json_header)
        except:
            print("Failed to crawl JSON URL: {0}, Retrying.".format(json_url))
            return self.extract_next_urls(json_url)

        html_str = url_response.content.decode()
        url_data = json.loads(html_str)["topic_list"]["topics"]

        res, base = [], "https://community.cloudflare.com/t/"

        for data_line in url_data:
            id, slug = data_line["id"], data_line["slug"]
            new_url = base + slug + "/" + str(id)
            res.append([new_url, id, slug])

        return res

    def external_crawl(self, included_url):
        bList = ["png", "jpg", "pdf"]
        for ending in bList:
            if ending in included_url:
                return
        if self.already_met[included_url] == 1:
            return
        self.already_met[included_url] = 1

        print("PID {2} | Crawling External URL: {0} Crawled: {1}".format(included_url, self.crawled_count, self.pid))
        url_response = self.handle_url(included_url, 1)
        time.sleep(self.crawl_interval)

        if not url_response:
            return

        self.store_external(url_response, included_url)

    def retry_external(self):
        while self.external_failed_queue:
            external_url = self.external_failed_queue.popleft()

            print("PID {2} | Retrying External URL: {0} Retried times: {1}".format(external_url, self.external_failed_dict[external_url], self.pid))
            self.external_failed_dict[external_url] += 1
            url_response = self.retry_url(external_url, 1)
            time.sleep(self.crawl_interval)

            if not url_response:
                continue

            self.store_external(url_response, external_url)

    def store_external(self, url_response, included_url):
        soup = BeautifulSoup(url_response.text, "lxml")
        try:
            title = soup.title.string
        except:
            return

        insert_to_db = {"title": title,
                        "url": included_url,
                        "content": url_response.text}
        self.collection.insert_one(insert_to_db)

# A simple refiner that retrieves the post contents from the HTML file.
class refiner_cloudflare:
    def __init__(self, html_response, cur_url):
        self.response = html_response
        self.cur_url = cur_url
        self.post_content = ""
        self.included_url = []
        self.disallow = ["/admin/", "/auth/", "/email/", "/session",
                         "/user-api-key", "/badges", "/u/", "/my", "/search",
                         "/g", ".rss"]

    def refine(self):
        soup1 = BeautifulSoup(self.response.text, "lxml")
        divs = soup1.find_all("div", {"class": "post"})

        self.post_content = ""

        for div in divs:
            self.post_content += str(div)

        soup2 = BeautifulSoup(self.post_content, "lxml")
        for anchor in soup2.find_all("a"):
            new_url = anchor.get("href")
            abs_url = parse.urljoin(self.cur_url, new_url)

            for disallow_element in self.disallow:
                if disallow_element in abs_url:
                    continue
                if "http" not in abs_url:
                    continue
            self.included_url.append(abs_url)


def crawl_multiP(url):
    myCrawler = Crawler_cloudflare()
    myCrawler.start_crawling(url)

if __name__ == "__main__":
    cpu_num = multiprocessing.cpu_count()
    P_pool = Pool(cpu_num)

    base_url = "https://community.cloudflare.com/latest.json?no_definitions=true&page="
    # for pageNum in range(28, 55): # Closed posts in the last month

    # pageNum for crawling
    for pageNum in range(1, 6):
        url = base_url + str(pageNum)
        P_pool.apply_async(crawl_multiP, args=(url,))

    P_pool.close()
    P_pool.join()

