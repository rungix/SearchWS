try:
    import cPickle as pickle
except ImportError:
    import pickle
import zlib
from datetime import datetime, timedelta
from pymongo import MongoClient
from bson.binary import Binary


class MongoWSURL:
    """
    Wrapper around MongoDB to cache downloads

    >>> cache = MongoWSURL()
    >>> cache.clear()
    >>> url = 'http://example.webscraping.com'
    >>> result = {'html': '...'}
    >>> cache[url] = result
    >>> cache[url]['html'] == result['html']
    True
    >>> cache = MongoWSURL(expires=timedelta())
    >>> cache[url] = result
    >>> # every 60 seconds is purged http://docs.mongodb.org/manual/core/index-ttl/
    >>> import time; time.sleep(60)
    >>> cache[url]
    Traceback (most recent call last):
     ...
    KeyError: 'http://example.webscraping.com does not exist'
    """
    def __init__(self, client=None, expires=timedelta(days=30)):
        """
        client: mongo database client
        expires: timedelta of amount of time before a cache entry is considered expired
        """
        # if a client object is not passed
        # then try connecting to mongodb at the default localhost port
        self.client = MongoClient('localhost', 27017, connect=False) if client is None else client
        #create collection to store cached ws_urls,
        # which is the equivalent of a table in a relational database
        self.db = self.client.searchws
        self.db.ws_urls.create_index('timestamp', expireAfterSeconds=expires.total_seconds())

    def __contains__(self, url):
        try:
            self[url]
        except KeyError:
            return False
        else:
            return True

    def __getitem__(self, url):
        """Load value at this URL
        """
        record = self.db.ws_urls.find_one({'_id': url})
        if record:
            return record['result']
            # return pickle.loads(zlib.decompress(record['result']))
        else:
            raise KeyError(url + ' does not exist')


    def __setitem__(self, url, result):
        """Save value for this URL
        """
        record = {'result': result, 'timestamp': datetime.utcnow()}
        # record = {'result': Binary(zlib.compress(pickle.dumps(result))), 'timestamp': datetime.utcnow()}
        self.db.ws_urls.update({'_id': url}, {'$set': record}, upsert=True)


    def clear(self):
        self.db.ws_urls.drop()
