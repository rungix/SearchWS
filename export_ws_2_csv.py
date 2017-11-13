import pymongo
from pymongo import MongoClient
from termcolor import colored
# from pprint import pprint
import csv
import os

if __name__ == '__main__':
    print("pymongo version: ", pymongo.version)
    client = MongoClient('localhost', 27017, connect=False)
    db = client.searchws
    ws_urls = db.ws_urls
    for ws_cursor in ws_urls.find({}):
        # pprint(ws_urls_set)
        origin = ws_cursor['_id']
        ws_set = ws_cursor['result']['ws_urls']
        with open('ws_urls.csv', 'ab') as ws_file:
            try:
                for i in range(len(ws_set)):
                    ws_url = ws_set.pop()
                    print(">>%d<< origin: %s   ws: %s" % (i, colored(str(origin), 'green'), colored(ws_url, 'red')))
                    result = str(origin) + ',' + ws_url
                    ws_file.write(result)
                    ws_file.write('\n')
            except Exception as e: # errors.DuplicateKeyError as e:
                print("Error: ", e)
                pass # this is already in the queue
    os.system('cat  ws_urls.csv | awk -F "," "{print $2}" | sort  | uniq -c')
    os.system('cat  ws_urls.csv | wc')
    os.system('cat  ws_urls.csv | awk -F "," "{print $2}" | sort  | uniq -c | wc')
