# -*- coding: utf-8 -*-

import csv
from zipfile import ZipFile
from StringIO import StringIO
from mongo_cache import MongoCache
import urllib2


class AlexaCallback:
    def __init__(self, max_urls=1000):
        self.max_urls = max_urls

    def __call__(self, seed_url):
        print("Download Alexa TOP " + seed_url)
        opener = urllib2.build_opener()
        response = opener.open(seed_url)
        zipped_data = response.read()
        zipped_code = response.code
        print("Download Alexa response code: %d " % zipped_code)
        cache = MongoCache()
        urls = []
        with ZipFile(StringIO(zipped_data)) as zf:
            csv_filename = zf.namelist()[0]
            for _, website in csv.reader(zf.open(csv_filename)):
                if 'http://' + website not in cache:
                    urls.append('http://' + website)
                    if len(urls) == self.max_urls:
                        break
        # urls=['https://docs.python.org/']
        return urls
