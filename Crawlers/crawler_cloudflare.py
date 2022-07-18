# imports for the crawler
import requests as r
import os
import time
import json
import multiprocessing
from multiprocessing import Pool

from collections import deque, defaultdict

# imports for the parser
from bs4 import BeautifulSoup
from urllib import parse

# imports for mongodb
import pymongo

# imports for parser
import cloudflare_parser


class Crawler_cloudflare:
    def __init__(self):
        self.pid = 0
        self.crawled_count = 0

        self.crawl_interval = 25
        self.header = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                       "Accept-Language": "en-US;q=0.9,en;q=0.8"}
        self.json_header = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                            "Accept": "application/json"}

        self.failed_queue = deque([])
        self.external_failed_queue = deque([])
        self.failed_dict = defaultdict(int)
        self.external_failed_dict = defaultdict(int)

        self.json_page_num = ""

        self.refined_collection = None
        self.raw_collection = None
        self.pure_collection = None

        self.refined_collection_external = None
        self.raw_collection_external = None
        self.pure_collection_external = None

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

        self.refined_collection = db["refined" + collection_name]
        self.raw_collection = db["raw" + collection_name]
        self.pure_collection = db["pure" + collection_name]

        self.refined_collection_external = db["refined" + collection_name + "_external"]
        self.raw_collection_external = db["raw" + collection_name + "_external"]
        self.pure_collection_external = db["pure" + collection_name + "_external"]

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
            if self.external_failed_dict[url_retry] < 2:
                self.external_failed_queue.append(url_retry)
            else:
                print("PID {1} | Retry for External URL: {0} FAILED.".format(url_retry, self.pid))
        return ""

    def retry_url(self, url_retry, isExternal):
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
        title = slug + "_" + str(id)
        insert_to_db_raw = {"title": title,
                            "url": real_url,
                            "raw_content": url_response.text}
        self.raw_collection.insert_one(insert_to_db_raw)


        refiner = refiner_cloudflare(url_response, real_url, self.header)
        refiner.refine()
        insert_to_db_refined = {"title": title,
                        "url": real_url,
                        "closed": refiner.closed}

        insert_to_db_pure = {"title": title,
                             "url": real_url,
                             "closed": refiner.closed}


        index, user_list = 0, []
        original_post, created_date = "", ""
        replies, replies_pure = {}, {}
        for info in refiner.refined_content:
            user = info[0].replace(".", "_")
            if user not in user_list:
                user_list.append(user)
            if index == 0:
                original_post = info[2]
                created_date = info[1]
            else:
                if user not in replies:
                    replies[user] = []
                if user not in replies_pure:
                    replies_pure[user] = []

                replies[user].append({"date" : info[1],
                                      "reply" : info[2]})

                cParser = cloudflare_parser.HTML_DataHandler()
                pure_content = cParser.handle_htmldata(info[2])
                replies_pure[user].append({"date" : info[1],
                                      "reply" : pure_content})
            index += 1

        insert_to_db_refined["user_list"] = user_list
        insert_to_db_refined["original_post"] = original_post
        insert_to_db_refined["created_date"] = created_date
        insert_to_db_refined["post_creator"] = user_list[0]
        insert_to_db_refined["replies"] = {}
        for r in replies:
            insert_to_db_refined["replies"][r] = replies[r]
        self.refined_collection.insert_one(insert_to_db_refined)

        insert_to_db_pure["user_list"] = user_list
        cParser = cloudflare_parser.HTML_DataHandler()
        pure_post = cParser.handle_htmldata(original_post)
        insert_to_db_pure["original_post"] = pure_post
        insert_to_db_pure["created_date"] = created_date
        insert_to_db_pure["post_creator"] = user_list[0]
        insert_to_db_pure["replies"] = {}
        for r in replies_pure:
            insert_to_db_pure["replies"][r] = replies_pure[r]
        self.pure_collection.insert_one(insert_to_db_pure)

        # if refiner.included_url:
        #     for external_url in refiner.included_url:
        #         self.external_crawl(external_url)
        #
        # self.retry_external()

    def extract_next_urls(self, json_url):
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
        bList = ["png", "jpg", "pdf", "jpeg"]
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
            title = "Title: Unknown"

        insert_to_db_raw = {"title": title,
                        "url": included_url,
                        "raw_content": url_response.text}
        self.raw_collection_external.insert_one(insert_to_db_raw)

        container = soup.find_all("div")
        divcontent = str(container)

        soup2 = BeautifulSoup(divcontent, "lxml")
        pcontainer = soup2.find_all("p")
        pcontent = str(pcontainer)

        insert_to_db_refined = {"title": title,
                                "url": included_url,
                                "refined_content": pcontent}
        self.refined_collection_external.insert_one(insert_to_db_refined)

        cParser = cloudflare_parser.HTML_DataHandler()
        content = cParser.handle_htmldata(pcontent)
        insert_to_db_pure = {"title": title,
                        "url": included_url,
                        "pure_text": content}
        self.pure_collection_external.insert_one(insert_to_db_pure)


# A simple refiner that retrieves the post contents from the HTML file.
class refiner_cloudflare:
    def __init__(self, html_response, cur_url, headers):
        self.response = html_response
        self.cur_url = cur_url
        self.headers = headers
        self.refined_content = []
        self.included_url = []
        self.disallow = ["/admin/", "/auth/", "/email/", "/session",
                         "/user-api-key", "/badges", "/u/", "/my", "/search",
                         "/g", ".rss"]
        self.pageCount = 2
        self.long_posts = {}
        self.closed = False

    def refine_helper(self, divs):
        for div in divs:
            username = div.find("span", {"class": "creator"}).text.strip()
            postdate = div.find("time", {"class" : "post-time"}).text.strip()

            tempsoup = BeautifulSoup(str(div), "lxml")
            post = tempsoup.find("div", {"class" : "post"}).text.strip()

            self.refined_content.append([username, postdate, post])
        return

    def get_long_response(self, long_url):
        try:
            print("------- | Crawling Long-post URL: {0}".format(long_url))
            long_response = r.get(long_url, timeout=20, headers=self.headers)
            return long_response
        except:
            print("------- | Retrying Long-post URL: {0}".format(long_url))
            time.sleep(25)
            return self.get_long_response(long_url)

    def handle_long_post(self):
        soup1 = BeautifulSoup(self.response.text, "lxml")
        divs = soup1.find_all("div", {"class": "topic-body crawler-post"})
        if "role" in divs[-1].attrs:
            self.refine_helper(divs[:-1])
        else:
            self.refine_helper(divs)

        p = soup1.find_all("div", {"class": "post"})
        self.extract_external_url(str(p))

        long_div = soup1.find("div", {"role": "navigation"})
        while long_div and "next" in long_div.text.strip():
            long_url = self.cur_url + "?page=" + str(self.pageCount)
            long_response = self.get_long_response(long_url)

            soup_long = BeautifulSoup(long_response.text, "lxml")
            divs_long = soup_long.find_all("div", {"class": "topic-body crawler-post"})

            if "role" in divs_long[-1].attrs:
                self.refine_helper(divs_long[:-1])
            else:
                self.refine_helper(divs_long)

            closed_divs = soup_long.find_all("span", {"class": "creator", "itemprop": "author"})
            if not closed_divs:
                pass
            else:
                self.closed = "closed" in closed_divs[-1].text

            p_long = soup_long.find_all("div", {"class": "post"})
            self.extract_external_url(str(p_long))
            long_div = soup_long.find("div", {"role": "navigation"})
            self.pageCount += 1


    def refine(self):
        soup1 = BeautifulSoup(self.response.text, "lxml")

        long_post = False
        check_for_long_post = soup1.find("div", {"role" : "navigation"})
        if check_for_long_post:
            if "next" in check_for_long_post.text.strip():
                long_post = True

        if long_post:
            self.handle_long_post()

        if not long_post:
            p = soup1.find_all("div", {"class": "post"})
            post_content_string = str(p)

            divs = soup1.find_all("div", {"class": "topic-body crawler-post"})
            if "role" in divs[-1].attrs:
                self.refine_helper(divs[:-1])
            else:
                self.refine_helper(divs)

            closed_divs = soup1.find_all("span", {"class": "creator", "itemprop": "author"})
            if not closed_divs:
                pass
            else:
                self.closed = "closed" in closed_divs[-1].text
            self.extract_external_url(post_content_string)


    def extract_external_url(self, post_content_string):
        soup2 = BeautifulSoup(post_content_string, "lxml")
        for anchor in soup2.find_all("a"):
            new_url = anchor.get("href")
            if not new_url:
                continue

            abs_url = parse.urljoin(self.cur_url, new_url)

            disallowed = False
            for disallow_element in self.disallow:
                if disallow_element in abs_url:
                    disallowed = True

            if disallowed == True:
                continue

            if abs_url.find("http") != 0:
                continue
            if abs_url not in self.included_url:
                self.included_url.append(abs_url)


def crawl_multiP(url):
    myCrawler = Crawler_cloudflare()
    myCrawler.start_crawling(url)

if __name__ == "__main__":
    cpu_num = multiprocessing.cpu_count()
    P_pool = Pool(cpu_num)

    base_url = "https://community.cloudflare.com/latest.json?no_definitions=true&page="
    #for pageNum in range(28, 55): # Closed posts in the last month

    #pageNum for crawling
    for pageNum in range(29, 31):
        url = base_url + str(pageNum)
        P_pool.apply_async(crawl_multiP, args=(url,))
        time.sleep(10)

    P_pool.close()
    P_pool.join()



