import pymongo
from nltk.stem import WordNetLemmatizer
from collections import defaultdict

def main(startPage, endPage):
    client = pymongo.MongoClient(host="128.195.180.83",
                                 port=27939,
                                 username="db_writer",
                                 password="ucidsplab_dbwriter"
                                 )
    #dblist = client.list_database_names() #print(dblist)
    db = client.cloudflare_crawled_data

    for i in range(startPage, endPage + 1):
        col_name = "purepage" + str(i)
        collection = db[col_name]

        for row in collection.find():
            print(row["_id"])
            filter, labels = {"_id": row["_id"]}, {"labels": tagger(row["original_post"])}
            collection.update_one(filter, {"$set": labels})


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


def tag1GeneralDNS(content_dict):
    keywords = ["dns", "domain", "server", "network"]
    for keyword in keywords:
        if keyword in content_dict:
            return True
    return False


def tag2Records(content_dict, two_grams):
    records = ["A record", "AAAA record", "CNAME record", "MX record", "TXT record", "NS record",
               "SOA record", "SRV record", "PTR record"]
    lesscommon_records = ["AFSDB record", "APL record", "CAA record", "DNSKEY record", "CDNSKEY record",
                          "CERT record", "DCHID record", "DNAME record", "HIP record", "IPSECKEY record",
                          "LOC record", "NAPTR record", "NSEC record", "RRSIG record", "RP record", "SSHFP record"]
    result = set()
    for record in records:
        if record in two_grams or record.lower() in two_grams:
            result.add(record)

        abbreviation = record.split(" ")[0].lower()
        if abbreviation in content_dict:
            result.add(record)

    for record in lesscommon_records:
        if record in two_grams or record.lower() in two_grams:
            result.add(record)

        abbreviation = record.split(" ")[0].lower()
        if abbreviation in content_dict:
            result.add(record)

    return list(result)


def tag3ServerLevels(content_dict, two_grams):
    result = []
    if "root" in content_dict:
        result.append("Root Level")
    if "host" in content_dict:
        result.append("Host")
    if "tld" in content_dict or "top level" in two_grams:
        result.append("TLD")
    if "sld" in content_dict or "second level" in two_grams:
        result.append("SLD")
    if "subdomain" in content_dict or "sub domain" in two_grams:
        result.append("Sub-Domain")

    return result


def tag4Security(content_dict):
    keywords = ["security", "firewall", "block", "filter", "gateway", "ddos", "dos", "hijack",
                "tunnel", "ssl", "tls", "ssl tls", "https", "malware", "attack", "prevent", "cert", "certificate", "expose",
                "forbid", "authentication", "protect", "phishing", "captcha"]
    for keyword in keywords:
        if keyword in content_dict:
            return True
    return False


def tag5ResolveTypes(content_dict):
    result = []
    if "recursive" in content_dict:
        result.append("Recursive Resolution")
    if "iterative" in content_dict:
        result.append("Iterative Resolution")
    return result


def tag6Ip(content_dict):
    result = []
    if "ipv4" in content_dict:
        result.append("IPv4")
    if "ipv6" in content_dict:
        result.append("IPv6")

    if not result and "ip" in content_dict:
        result.append("General IP")
    return result

def tag7HTTPstatusCode(content_dict):
    info_response = {str(_): 1 for _ in range(100, 104)}

    success_response = {str(_): 1 for _ in range(200, 209)}
    success_response.update({"226" : 1})

    redir_message = {str(_): 1 for _ in range(300, 309)}

    client_error = {str(_): 1 for _ in range(400, 432)}
    not_used = ["419", "420", "427", "430"]
    for code in not_used:
        del client_error[code]
    client_error["451"] = 1

    servre_error = {str(_): 1 for _ in range(500, 512)}

    result = []
    for key in content_dict:
        if key in info_response or key in success_response or key in redir_message or key in client_error or\
            key in servre_error:
            result.append(key)
    return result

def tag8ErrorCode(two_grams):
    result = []
    for bigram in two_grams:
        if len(bigram) > 4 and bigram[:4] == "code":
            code = None
            try:
                code = int(bigram[5:])
            except:
                pass

            if code:
                result.append(bigram)

    return result


def tagger(pure_content):
    labels = []
    content_dict, two_grams = tokenize(pure_content)

    # 1.General
    if tag1GeneralDNS(content_dict):
        labels.append("General DNS & Network Issues")
        categoryToPostNum["1.General"] += 1

    # 2.Record types
    indicator = -1
    for record in tag2Records(content_dict, two_grams):
        labels.append(record)
        indicator = 1
    if indicator == 1:
        categoryToPostNum["2.Record types"] += 1

    # 3.Server levels
    indicator = -1
    for level in tag3ServerLevels(content_dict, two_grams):
        labels.append(level)
        indicator = 1
    if indicator == 1:
        categoryToPostNum["3.Server levels"] += 1

    # 4.Security
    if tag4Security(content_dict):
        labels.append("Security")
        categoryToPostNum["4.Security"] += 1

    # 5.Resolving Method
    indicator = -1
    for method in tag5ResolveTypes(content_dict):
        labels.append(method)
        indicator = 1
    if indicator == 1:
        categoryToPostNum["5.Resolving Method"] += 1

    # 6.IP
    indicator = -1
    for ip in tag6Ip(content_dict):
        labels.append(ip)
        indicator = 1
    if indicator == 1:
        categoryToPostNum["6.IP"] += 1

    # 7.HTTP Status Code
    # for code in tag7HTTPstatusCode(content_dict):
    #     labels.append(code)
    # Temporarily disabled because of irrelevance for DNS

    # 8.Error Code
    for error_code in tag8ErrorCode(two_grams):
        labels.append(error_code)

    # Others
    if not labels:
        labels.append("Others")
        categoryToPostNum["7.Others"] += 1

    tagNum = len(labels)
    tagNumToPostNum[tagNum] += 1

    for l in labels:
        tagToPostNum[l] += 1


    return labels


if __name__ == "__main__":
    lemmatizer = WordNetLemmatizer()
    tagNumToPostNum = defaultdict(int)
    tagToPostNum = defaultdict(int)
    categoryToPostNum = defaultdict(int)
    start, end = 34, 84
    main(start, end)
    num = (end - start + 1) * 30
    print("====================================================")
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
