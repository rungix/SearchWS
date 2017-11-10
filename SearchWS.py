# -*- coding: utf-8 -*-

import sys
from process_crawler import process_crawler
from mongo_cache import MongoCache
from alexa_cb import AlexaCallback
from mongo_queue import MongoQueue


def main(max_threads):
    cache = MongoCache()
    cache.clear()
    crawl_queue = MongoQueue()
    crawl_queue.clear()
    seed_url = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
    alexa = AlexaCallback(max_urls=4)
    try:
        links = alexa(seed_url) or []
    except Exception as e:
        print('Error in downing {}: {}'.format(seed_url, e))
    else:
        for link in links:
            crawl_queue.push(link)
    process_crawler(cache=cache, max_threads=max_threads, timeout=10)


if __name__ == '__main__':
    max_threads = int(sys.argv[1])
    main(max_threads)
