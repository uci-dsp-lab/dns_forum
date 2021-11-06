import pymongo
from nltk.stem import WordNetLemmatizer

def main(startPage, endPage):
    client = pymongo.MongoClient(host="128.195.180.83",
                                 port=27017,
                                 username="db_writer",
                                 password="ucidsplab_dbwriter"
                                 )
    #dblist = client.list_database_names() #print(dblist)
    db = client.cloudflare_crawled_data

    for i in range(startPage, endPage + 1):
        col_name = "purepage" + str(i)
        collection = db[col_name]

        for row in collection.find():
            #print(row["_id"], row["original_post"])
            print(row["_id"])
            filter, labels = { "_id": row["_id"] }, { "labels": tagger(row["original_post"]) }
            collection.update_one( filter, { "$set" : labels } )

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

def tag2Records(two_grams):
    records = ["A record", "AAAA record", "CNAME record", "MX record", "TXT record", "NS record"]
    result = []
    for record in records:
        if record in two_grams or record.lower() in two_grams:
            result.append(record)
    return result

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
    keywords = ["firewall", 'blocked', "gateway", "api", "ssl", "malware", "attack", "ddos"]
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

def tagger(pure_content):
    labels = []
    content_dict, two_grams = tokenize(pure_content)

    # 1.General
    if tag1GeneralDNS(content_dict):
        labels.append("General DNS & Network Issues")

    # 2.Record types
    for record in tag2Records(two_grams):
        labels.append(record)

    # 3.Server levels
    for level in tag3ServerLevels(content_dict, two_grams):
        labels.append(level)

    # 4.Security
    if tag4Security(content_dict):
        labels.append("Security")

    # 5.Resolving Method
    for method in tag5ResolveTypes(content_dict):
        labels.append(method)

    if not labels:
        labels.append("Others")
    return labels

if __name__ == "__main__":
    lemmatizer = WordNetLemmatizer()
    main(35, 40)