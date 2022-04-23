import pymongo
from nltk.stem import WordNetLemmatizer
from collections import defaultdict

def main(startPage, endPage):
    num = 0
    client = pymongo.MongoClient(host="128.195.180.83",
                                 port=27939,
                                 username="db_writer",
                                 password="ucidsplab_dbwriter")

    db = client.cloudflare_crawled_data

    count = []
    dnsRelatedNum = 0
    for i in range(startPage, endPage + 1):
        col_name = "purepage" + str(i)
        collection = db[col_name]

        for row in collection.find():
            row["original_post"] = row["original_post"].replace("\n", " ") # replace the newline signal
            row["original_post"] = row["title"].replace("-", " ").replace("_", " ") + ' '+row["original_post"]
#             print(row)
            for reply in row["replies"]:
#                 print(reply)
                for single_reply in row["replies"][reply]:
#                     print(i)
                    row["original_post"] += ' ' + single_reply["reply"]
#             exit()
            if not row["DNS_Related"]:
                continue
            if "Other Languages" in row["labels"]:
                continue
            date = row["created_date"].split(",")
            year = date[1][1:5]
            if year != "2021" and year != "2020":
                continue

            print(row["_id"], end="\r")
#             print(row)
#             print(row["original_post"])
#             exit()

            tags = tagger(row["original_post"], row, count)
            if not tags:
                continue

            filter, labels = {"_id": row["_id"]}, {"labels_v2": tags[0]}

            dnsRelatedNum += 1
            collection.update_one(filter, {"$set": labels})

    print(len(count))
    print("Total DNS Related Post:", dnsRelatedNum)
    return num

# two_grams: Try each consecutive words and save them in a dict
def tokenize(content_string):
    content_list = content_string.split(" ")
    content_dict, two_grams = {}, {}

    for i in range(len(content_list)):
        word = content_list[i].lower()
        token = lemma(word)

        if token not in content_dict:
            content_dict[token] = 0
        content_dict[token] += 1

        if i + 1 < len(content_list):
            next_token = lemma(content_list[i + 1].lower())
            two_gram = token + " " + next_token
            if two_gram not in two_grams:
                two_grams[two_gram] = 0
            two_grams[two_gram] += 1
    return content_dict, two_grams





def lemma(word):
    word = word.strip()

    # do not lemma these words:
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


def tag1NetworkConnectivity(content_dict, two_grams):
    keywords = ["network", "connect", "connectivity", "connection", "slow", "block", "time", "timeout", "fail", "client", "warp"]
    bigrams = ["time out"]
    for keyword in keywords:
        if keyword in content_dict:
            return True
    for bg in bigrams:
        if bg in two_grams:
            return True
    return False

def tag2Management(content_dict, two_grams):
#     keywords = ["registration", "registrar", "register", "nameserver", "nameservers", "account", "transfer",
#                 "mydomain", "whois", "godaddy", "icann", "iana", "host"]
    keywords = ["registration", "registrar", "register", "nameserver", "nameservers", "account", "transfer",
                "mydomain", "whois", "godaddy", "icann", "iana", "host"]
    bigrams = ["go daddy"]
    for keyword in keywords:
        if keyword in content_dict:
            return True
    for bg in bigrams:
        if bg in two_grams:
            return True
    return False

def tag3Security(content_dict, two_grams):
    keywordsL1 = ["block", "filter", "tunnel", "https", "prevent", "expose", "forbid", "captcha", "safety", "insurance",
                  "guarantee", "warrant", "ward", "shelter", "trouble", "https", "proxy", "proxied"]

    keywordsL2 = ["security", "secure", "insecurity", "firewall", "ddos", "dos", "hijack",
                "ssl", "tls", "ssl tls", "malware", "attack", "cert", "certificate",
                "authentication", "protect", "phishing"]

    for keyword in keywordsL2:
        if keyword in content_dict:
            return True

    count = 0
    for keyword in keywordsL1:
        if keyword in content_dict:
            count += 1

    if count > 1:
        return True

    return False

def tag4Website(content_dict, two_grams):
    keywords = ["site", "website", "www", "http", "https", "webpage", "page", "display", "certificate", "redirect", "ip"]
    for keyword in keywords:
        if keyword in content_dict:
            return True
    return False

def tagger(pure_content, row, count):
    labels = []
    content_dict, two_grams = tokenize(pure_content)
#     print(content_dict, '\n\n\n',two_grams)
#     exit()
    if tag1NetworkConnectivity(content_dict, two_grams):
        labels.append("Network Connectivity")
    if tag2Management(content_dict, two_grams):
        labels.append("Management")
    if tag3Security(content_dict, two_grams):
        labels.append("Security")
    if tag4Website(content_dict, two_grams):
        labels.append("Website")

    tagNum = len(labels)
#     if tagNum == 0:
#         print(row)
#         exit()
    tagNumToPostNum[tagNum] += 1

    for l in labels:
        tagToPostNum[l] += 1

    return labels


if __name__ == "__main__":
    lemmatizer = WordNetLemmatizer()
    tagNumToPostNum = defaultdict(int)
    tagToPostNum = defaultdict(int)
    categoryToPostNum = defaultdict(int)

    start, end = 100, 1354

    num = main(start, end)
    print(tagNumToPostNum)
#     print("===========================`=========================") # Jason: useless?
#     print("Total Number of Posts:", num)
    print("====================================================")
    print("Number of Tags \t\t\t Number of Posts")
    for tagNum in sorted(tagNumToPostNum.keys()):
        print(str(tagNum) + "\t\t\t" + str(tagNumToPostNum[tagNum]))

    print("====================================================")

    print("Tag \t\t\t Number of Posts")
    for tag in sorted(tagToPostNum.keys()):
        print(str(tag) + "\t\t\t" + str(tagToPostNum[tag]))

    print("====================================================")

    print("Category \t\t\t Number of Posts")
    for cate in sorted(categoryToPostNum.keys()):
        print(str(cate) + "\t\t\t" + str(categoryToPostNum[cate]))
