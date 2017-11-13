try:
    import cPickle as pickle
except ImportError:
    import pickle
import zlib
from datetime import datetime, timedelta
from pymongo import MongoClient
from bson.binary import Binary


class MongoCrawledURL:
    """
    Wrapper around MongoDB to cache URLs that has been crawled
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
        self.db.crawled_urls.create_index('timestamp', expireAfterSeconds=expires.total_seconds())

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
        record = self.db.crawled_urls.find_one({'_id': url})
        if record:
            return record['depth']
            # return pickle.loads(zlib.decompress(record['result']))
        else:
            raise KeyError(url + ' does not exist')


    def __setitem__(self, url, depth):
        """Save value for this URL
        """
        record = {'depth': depth, 'timestamp': datetime.utcnow()}
        # record = {'result': Binary(zlib.compress(pickle.dumps(result))), 'timestamp': datetime.utcnow()}
        self.db.crawled_urls.update({'_id': url}, {'$set': record}, upsert=True)


    def clear(self):
        self.db.crawled_urls.drop()
