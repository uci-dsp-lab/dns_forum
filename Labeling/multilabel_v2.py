import pymongo
from nltk.stem import WordNetLemmatizer
from collections import defaultdict

def refine(title):
    title = title.replace("-", " ")
    title = title.replace("_", " ")
    return title

def main(startPage, endPage):
    num = 0
    client = pymongo.MongoClient(host="128.195.180.83",
                                 port=27939,
                                 username="db_writer",
                                 password="ucidsplab_dbwriter"
                                 )

    db = client.cloudflare_crawled_data


    count = []
    dnsRelatedNum = 0
    for i in range(startPage, endPage + 1):
        col_name = "purepage" + str(i)
        collection = db[col_name]

        for row in collection.find():

            if not row["DNS_Related"]:
                continue
            if "Other Languages" in row["labels"]:
                continue
            date = row["created_date"].split(",")
            year = date[1][1:5]
            if year != "2021" and year != "2020":
                continue
            if row["Non_English"]:
                continue

            dnsRelatedNum += 1
            print(row["_id"])

            tags = tagger(row["original_post"], row, count)
            for t in tagger(refine(row["title"]), row, count):
                if t not in tags:
                    tags.append(t)


            filter, labels = {"_id": row["_id"]}, {"labels_v2": tags}
            if not tags:
                collection.update_one(filter, {"$set": {"labels_v2": ["Unclassified"]}})
                collection.update_one(filter, {"$set": {"updated": True}})
                tagNumToPostNum[0] += 1
                continue

            #dnsRelatedNum += 1
            collection.update_one(filter, {"$set": labels})
            collection.update_one(filter, {"$set": {"updated":True}})

            tagNum = len(tags)
            tagNumToPostNum[tagNum] += 1

            for l in tags:
                tagToPostNum[l] += 1


    print("Total DNS Related Post:", dnsRelatedNum)
    return num


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
    print(content_dict)
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
    keywordsT1 = ["slow", "fail", "block", "time", "traffic", "packet", "refuse", "masquerade", "visitor",
                  "transmit", "performance", "load", "direct", "stable"]
    keywordsT2 = ["network", "connect", "connectivity", "connection", "timeout", "client", "warp"]
    bigrams = ["time out", "rate limit", "packet loss", "refuse connection"]

    count = 0
    for keyword in keywordsT1:
        if keyword in content_dict:
            count += 1
    if count >= 2:
        return True

    for keyword in keywordsT2:
        if keyword in content_dict:
            return True

    for bg in bigrams:
        if bg in two_grams:
            return True
    return False

def tag2Management(content_dict, two_grams):
    keywords = ["registration", "registrar", "register", "nameserver", "nameservers", "account", "transfer",
                "mydomain", "whois", "godaddy", "icann", "iana", "bluehost"]
    bigrams = ["go daddy", "name server"]
    for keyword in keywords:
        if keyword in content_dict:
            return True
    for bg in bigrams:
        if bg in two_grams:
            return True
    return False

def tag3Security(content_dict, two_grams):
    keywordsT1 = ["block", "filter", "tunnel", "https", "prevent", "expose", "forbid", "captcha", "safety", "insurance",
                  "guarantee", "warrant", "ward", "shelter", "trouble", "https", "proxy", "proxied", "feign", "intercept",
                  "cert", "certificate"]

    keywordsT2 = ["security", "secure", "insecurity", "insecure", "firewall", "ddos", "dos", "hijack",
                "ssl", "tls", "ssl tls", "malware", "attack", "authentication", "authenticate", "protect", "phishing"]
    count = 0
    for keyword in keywordsT1:
        if keyword in content_dict:
            count += 1
    if count >= 3:
        return True

    for keyword in keywordsT2:
        if keyword in content_dict:
            return True

    return False

def tag4Website(content_dict, two_grams):
    keywordsT1 = ["display", "redirect", "bypass", "ip", "browser", "load", "response", "responsiveness", "visual",
                  "optimize", "optimization", "setup", "cdn"]
    keywordsT2 = ["site", "website", "www", "http", "https", "webpage", "page", "certificate", "cert", "redirect"]

    count = 0
    for keyword in keywordsT1:
        if keyword in content_dict:
            count += 1
    if count >= 2:
        return True

    for keyword in keywordsT2:
        if keyword in content_dict:
            return True
    return False

def tag5Resolve(content_dict, two_grams):
    keywords = ["resolve", "resolver", "resolvers"]
    for keyword in keywords:
        if keyword in content_dict:
            return True
    return False

def tag6Email(content_dict, two_grams):
    keywords = ["email", "mail"]
    for keyword in keywords:
        if keyword in content_dict:
            return True
    return False

def tagger(pure_content, row, count):
    labels = []
    content_dict, two_grams = tokenize(pure_content)

    if tag1NetworkConnectivity(content_dict, two_grams):
        labels.append("Network Connectivity")
    if tag2Management(content_dict, two_grams):
        labels.append("Management")
    if tag3Security(content_dict, two_grams):
        labels.append("Security")
    if tag4Website(content_dict, two_grams):
        labels.append("Website")
    if tag5Resolve(content_dict, two_grams):
        labels.append("DNS Resolve Related")
    if tag6Email(content_dict, two_grams):
        labels.append("Email Related")

    return labels


if __name__ == "__main__":
    lemmatizer = WordNetLemmatizer()
    tagNumToPostNum = defaultdict(int)
    tagToPostNum = defaultdict(int)
    categoryToPostNum = defaultdict(int)

    start, end = 100, 1354

    num = main(start, end)
    print("===========================`=========================")
    print("Total Number of Posts:", num)
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
