# -*- coding: utf-8 -*-

import sys
from process_crawler import process_crawler
from mongo_ws_urls import MongoWSURL
from alexa_cb import AlexaCallback
from mongo_crawling_queue import MongoCrawlingQueue
from mongo_crawled_url import MongoCrawledURL
import argparse

def main(max_threads=4, max_websites=3, depth=3):
    ws_urls_cache = MongoWSURL()
    ws_urls_cache.clear()
    crawling_queue = MongoCrawlingQueue()
    crawling_queue.clear()
    crawled_urls = MongoCrawledURL()
    crawled_urls.clear()
    seed_url = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
    alexa = AlexaCallback(max_websites)
    try:
        links = alexa(seed_url) or []
    except Exception as e:
        print('Error in downing {}: {}'.format(seed_url, e))
    else:
        for link in links:
            crawling_queue.push(link, depth)
    process_crawler(ws_urls_cache=ws_urls_cache, max_threads=max_threads, timeout=10,
                    depth=depth)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='A Simple Crawler')
    parser.add_argument('-t', '--threads', dest='threads',
                        help='number of threads per processor',
                        required='True', type=int, default='4')
    parser.add_argument('-w', '--websites', dest='websites',
                        help='number of websites to crawl',
                        required='True', type=int, default='3')
    parser.add_argument('-d', '--depth', dest='depth',
                        help='link depth when crawling',
                        required='True', type=int, default='3')
    args = parser.parse_args()
    main(args.threads, args.websites, args.depth)
