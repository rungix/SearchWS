import time
import urlparse
import threading
import multiprocessing
from mongo_cache import MongoCache
from mongo_queue import MongoQueue
from downloader import Downloader
import os

SLEEP_TIME = 1


def threaded_crawler(delay=5, cache=None, user_agent='SearchWS', proxies=None, num_retries=1, max_threads=10, timeout=60):
    """Crawl using multiple threads
    """
    # the queue of URL's that still need to be crawled
    crawl_queue = MongoQueue()
    # crawl_queue.clear()
    D = Downloader(cache=cache, delay=delay, user_agent=user_agent, proxies=proxies, num_retries=num_retries, max_depth=3, timeout=timeout)

    def process_queue():
        while True:
            # keep track that are processing url
            try:
                url = crawl_queue.pop()
            except KeyError:
                # currently no urls to process
                print("PID %d: currently no urls to process, thread exiting..." % os.getpid())
                break
            else:
                print("PID %d processes %s" % (os.getpid(), url))
                D(url)
                crawl_queue.complete(url)
                print("PID %d processed %s" % (os.getpid(), url))

    # wait for all download threads to finish
    threads = []
    while threads or crawl_queue.peek():
        # print("PID %d len of thread %d" % (os.getpid(), len(threads)))
        for thread in threads:
            if not thread.is_alive():
                threads.remove(thread)
        while len(threads) < max_threads and crawl_queue.peek():
            # can start some more threads
            print("PID %d starts new thread" % os.getpid())
            thread = threading.Thread(target=process_queue)
            thread.setDaemon(True) # set daemon so main thread can exit when receives ctrl-c
            thread.start()
            threads.append(thread)
        time.sleep(SLEEP_TIME)
        # print("PID %d sleeping..." % os.getpid())
    print("PID %d exiting..." % os.getpid())


def process_crawler(**kwargs):
    num_cpus = multiprocessing.cpu_count()
    #pool = multiprocessing.Pool(processes=num_cpus)
    print('>>>>> Starting {%d} processes' % (num_cpus))
    processes = []
    for i in range(num_cpus):
        p = multiprocessing.Process(target=threaded_crawler, kwargs=kwargs)
        #parsed = pool.apply_async(threaded_link_crawler, args, kwargs)
        p.start()
        processes.append(p)
    # wait for processes to complete
    for p in processes:
        p.join()


def normalize(seed_url, link):
    """Normalize this URL by removing hash and adding domain
    """
    link, _ = urlparse.urldefrag(link) # remove hash to avoid duplicates
    return urlparse.urljoin(seed_url, link)
