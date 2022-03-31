import pymongo
from nltk.stem import WordNetLemmatizer

def main(startPage, endPage):
    client = pymongo.MongoClient(host="128.195.180.83",
                                 port=27939,
                                 username="db_viewer",
                                 password="ucidsplab_dbviewer"
                                 )

    db = client.cloudflare_crawled_data
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

            tags = tagger(row["original_post"], row)
            if not tags:
                continue

            #collection.update_one(filter, {"$set": {"DNS_Related": DNS_Related}})
            filter = {"_id": row["_id"]}
            print(row["_id"])
            print(tags)
            dnsRelatedNum += 1

    print("Total Processed DNS Related Post:", dnsRelatedNum)


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
    # print(content_dict)
    # print(two_grams)
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

def resolverLocation(content_dict, two_grams):
    keywordList = ["root server", "tld server", "sld server", "stub resolver", "firewall",
                "forwarder", "gateway", "ingress server", "egress server", "isp resolver",
                "icann", "iana", "tld registry", "registrar"]

    result = set()
    for keyword in keywordList:
        if len(keyword.split(" ")) > 1:
            abbreviation = keyword.split(" ")[0]
            if abbreviation in content_dict:
                result.add(keyword)
        else:
            if keyword in content_dict:
                result.add(keyword)

        if keyword in two_grams:
            result.add(keyword)

    return list(result)


# def tagNetworking(conatent_dict, two_grams):
#     keywords = ["block", "limit", "redirect", "connectivity", "connection", "connect", "waf", "warp", "request"]
#     bigrams = ["time out", "redirect loop", "error message", "error code", "connection failure", "connectivity failure",
#                "rate limit", "invalid certificate", "security level"]
#
# def tagDomainMgmt(content_dict, two_grams):
#     keywords = ["registrar", "whois", "expire", "register", "goddady", "nameserver", "nameservers", "account", "transfer",
#                 "mydomain"]
#     bigrams = []
#
# def tagWebsite(content_dict, two_grams):
#     keywords = ["website", "site", "com", "www"]

def tagger(pure_content, row):
    content_dict, two_grams = tokenize(pure_content)

    if "dns" not in content_dict and "DNS & Network" not in row["raw_tag"] and "1.1.1.1" not in row["raw_tag"]:
        return

    #0 Checking foreign languages
    non_eng_count = 0
    for token in content_dict:
        if not isEng(token):
            non_eng_count += 1

    if non_eng_count >= 3:
        return

    return resolverLocation(content_dict, two_grams)

def isEng(s):
    try:
        s.encode(encoding='utf-8').decode('ascii')
    except UnicodeDecodeError:
        return False
    else:
        return True

if __name__ == "__main__":
    lemmatizer = WordNetLemmatizer()
    start, end = 100, 1354
    num = main(start, end)


