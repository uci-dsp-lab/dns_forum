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

# imports for parser
import cloudflare_parser

class Crawler_github:
    def __init__(self):
        self.pid = 0
        self.crawled_count = 0

        self.crawl_interval = 20
        self.header = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                       "Accept-Language": "en-US;q=0.9,en;q=0.8"}

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
        self.closed = True

    def start_crawling(self, current_url, pageNum, closed):
        self.closed = closed
        self.pid = os.getpid()
        print("PID {2} | Crawling URL: {0} Crawled: {1}".format(current_url, self.crawled_count, self.pid))
        self.crawled_count += 1

        client = pymongo.MongoClient(host="localhost", port=27017)
        db = client.gitlab_crawled_data_Bind9

        collection_name = "page" + str(pageNum)

        self.refined_collection = db["refined" + "_" + collection_name]
        self.raw_collection = db["raw" + "_" + collection_name]
        self.pure_collection = db["pure" + "_" + collection_name]

        self.refined_collection_external = db["refined" + "_" + collection_name + "_external"]
        self.raw_collection_external = db["raw" + "_" + collection_name + "_external"]
        self.pure_collection_external = db["pure" + "_" + collection_name + "_external"]

        url_queue = self.extract_next_urls(current_url)
        for url_info in url_queue:

            real_url, id = url_info[0], url_info[1]

            print("PID {2} | Crawling URL: {0} Crawled: {1}".format(real_url, self.crawled_count, self.pid))
            url_response = self.handle_url(real_url, 0)
            time.sleep(self.crawl_interval)

            if not url_response:
                continue

            self.store_crawled_data(url_response, id, real_url)

        self.retry()


    def retry(self):
        while self.failed_queue:
            url_retry = self.failed_queue.popleft()
            id = ""
            for i in range(len(url_retry) - 1, -1, -1):
                if url_retry[i] == "/":
                    break
                id = url_retry[i] + id

            print("PID {2} | Retrying URL: {0} Retried times: {1}".format(url_retry, self.failed_dict[url_retry], self.pid))
            self.failed_dict[url_retry] += 1
            url_response = self.retry_url(url_retry, 0)
            time.sleep(self.crawl_interval)

            if not url_response:
                continue

            self.store_crawled_data(url_response, id, url_retry)

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

    def store_crawled_data(self, url_response, id, real_url):
        refiner = refiner_gitlab(url_response, real_url, self.header)
        refiner.refine()
        title = refiner.title
        insert_to_db_raw = {"title": title,
                            "url": real_url,
                            "closed": True if self.closed else False,
                            "raw_content": url_response.text}
        self.raw_collection.insert_one(insert_to_db_raw)

        insert_to_db_refined = {"title": title,
                        "url": real_url,
                        "closed": True if self.closed else False}
        insert_to_db_pure = {"title": title,
                             "url": real_url,
                             "closed": True if self.closed else False,}

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

        insert_to_db_refined["external_urls"] = refiner.included_url
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
        # self.retry_external()

    def extract_next_urls(self, starting_url):
        try:
            url_response = r.get(starting_url, timeout=15, headers=self.header)
        except:
            print("Failed to crawl Starting URL: {0}, Retrying.".format(starting_url))
            return self.extract_next_urls(starting_url)

        res = []
        soup = BeautifulSoup(url_response.text, "lxml")
        visited, required, base_url = defaultdict(int), "/isc-projects/bind9/-/issues/", "https://gitlab.isc.org/"

        divs = soup.find_all("span", {"class" : "issue-title-text js-onboarding-issue-item"})
        for div in divs:
            new_url = div.find("a")["href"]

            post_id = ""
            for i in range(len(new_url)-1, -1, -1):
                if new_url[i] == "/":
                    break
                post_id = new_url[i] + post_id

            if visited[post_id] == 1 or required not in new_url:
                continue

            visited[post_id] = 1
            abs_url = parse.urljoin(base_url, new_url)
            res.append([abs_url, post_id])
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
        content = cParser.handle_htmldata(divcontent)
        insert_to_db_pure = {"title": title,
                        "url": included_url,
                        "pure_text": content}
        self.pure_collection_external.insert_one(insert_to_db_pure)


# A simple refiner that retrieves the post contents from the HTML file.
class refiner_gitlab:
    def __init__(self, html_response, cur_url, header):
        self.header = header
        self.response = html_response
        self.cur_url = cur_url
        self.title = ""
        self.post_content = ""
        self.refined_content = []
        self.included_url = []
        self.disallow = ["/autocomplete/users", "/autocomplete/projects", "/search", "/admin",
                         "/profile", "/dashboard", "/users", "/help", "/s/",
                         "/-/profile", "/-/ide/", "/new", "/edit", "/raw",
                         "/realtime_changes"]

    def handle_discussion(self, json_url):
        json_response = ""
        while not json_response:
            try:
                json_response = r.get(json_url, timeout=20, headers=self.header)
            except:
                time.sleep(20)
                json_response = ""

        json_str = json_response.content.decode()
        dis_data = json.loads(json_str)
        return dis_data

    def add_url(self, new_url):
        if not new_url:
            return

        disallowed = False
        for disallow_element in self.disallow:
            if disallow_element in new_url:
                disallowed = True

        if disallowed == True:
            return

        if new_url.find("http") != 0:
            return

        self.included_url.append(new_url)

    def refine(self):
        soup1 = BeautifulSoup(self.response.text, "lxml")

        try:
            self.title = soup1.title.string
        except:
            self.title = "Title: Unknown"

        divs = soup1.find_all("div", {"class" : "description"})
        self.post_content = str(divs)

        soupog = BeautifulSoup(str(divs[0]), "lxml")

        og_author = soup1.find_all("span", {"class":"author"})[0].text.replace(".","_")
        og_date = soup1.find_all("time", {"class": "js-timeago"})[0]["title"]
        og_content = soupog.text
        self.refined_content.append([og_author, og_date, og_content])


        note_url = self.cur_url + "/discussions.json"

        dis_data = self.handle_discussion(note_url)

        for note in dis_data:
            user = note["notes"][0]["author"]["name"].replace(".", "_")
            date = note["notes"][0]["updated_at"]
            if not date:
                date = note["notes"][0]["created_at"]
            reply = note["notes"][0]["note"]

            self.refined_content.append([user, date, reply])

            soup = BeautifulSoup(note["notes"][0]["note_html"], "lxml")
            url_divs = soup.find_all("a")
            for url_div in url_divs:
                new_url = url_div.get("href")
                self.add_url(new_url)

        soup2 = BeautifulSoup(self.post_content, "lxml")
        for anchor in soup2.find_all("a"):
            new_url = anchor.get("href")
            self.add_url(new_url)


def crawl_multiP(url, pageNum, closed):
    myCrawler = Crawler_github()
    myCrawler.start_crawling(url, pageNum, closed)


if __name__ == "__main__":
    cpu_num = multiprocessing.cpu_count()
    P_pool = Pool(cpu_num)

    # pageNum for crawling
    for pageNum in range(1, 2):
        url = "https://gitlab.isc.org/isc-projects/bind9/-/issues?page={0}&scope=all&state=closed".format(pageNum)
        P_pool.apply_async(crawl_multiP, args=(url, pageNum, True, ))
        time.sleep(8)

    for pageNum in range(1, 2):
        url = "https://gitlab.isc.org/isc-projects/bind9/-/issues?page={0}&scope=all&state=opened".format(pageNum)
        P_pool.apply_async(crawl_multiP, args=(url, pageNum, False, ))
        time.sleep(8)

    P_pool.close()
    P_pool.join()

