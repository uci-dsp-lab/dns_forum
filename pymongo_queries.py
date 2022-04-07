# import the library
import pymongo

# initialize connection to the Mongodb server; VPN is needed to connect successfully
client = pymongo.MongoClient(host="128.195.180.83",
                             port=27939,
                             username="db_viewer",
                             password="ucidsplab_dbviewer"
                             )

# direct to the database we want to check
db = client.cloudflare_crawled_data

# setting the page range we want to check:
startPage, endPage = 100, 200

# To search all the pages for forums:
# Cloudflare: 100, 1354
# OpenDNS: 1, 19
# Spiceworks: 1, 79
# Comodo: 1, 10

# go through those pages; each page is a collection
for i in range(startPage, endPage + 1):
    # concatenating to get the collection name
    col_name = "purepage" + str(i)

    # access the collection
    collection = db[col_name]

    # go through all the rows (posts) in that collection (page)
    for row in collection.find():
        # access certain entry by using this format row[entry_name]
        postUrl = row["url"]
        print(postUrl)
        # please notice that the data structure of entry "replies" is a nested dictionary

        # for label matching, search if a certain label is in row["labels"].
        # for example, search if IPv4 exists in the post's labels:
        if "IPv4" in row["labels"]:
            # If there is IPv4 in its labels, print out the post's mongodb id
            print(row["_id"])