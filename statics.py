import pymongo
from pymongo import MongoClient
from termcolor import colored

if __name__ == '__main__':
    print("pymongo version: ", pymongo.version)
    client = MongoClient('localhost', 27017, connect=False)
    db = client.searchws
    crawled_urls = db.crawled_urls
    ws_urls = db.ws_urls
    number_of_crawled_urls = crawled_urls.find({}).count()
    number_of_ws_urls = ws_urls.find({}).count()

    print("Crawled %s URLs and find %s WS urls" % (colored(number_of_crawled_urls, 'green'),
                                                   colored(number_of_ws_urls, 'red')))
