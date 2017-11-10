import urlparse
import urllib2
import random
import time
from datetime import datetime
import socket
import re
from termcolor import colored
import os
from os.path import splitext

DEFAULT_AGENT = 'SearchWS Agent'
DEFAULT_DELAY = 5
DEFAULT_RETRIES = 1
DEFAULT_DEPTH = 3
DEFAULT_TIMEOUT = 60


class Downloader:

    # http://www.noah.org/wiki/RegEx_Python
    WS_REGEX = 'ws[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
    # JS_REGEX = 'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
    JS_REGEX = r'\bsrc\b=[\"\'](.+?)[\"\']'
    WEB_REGEX = r'<a[^>]+href=["\'](.*?)["\']'

    def __init__(self, delay=DEFAULT_DELAY, user_agent=DEFAULT_AGENT,
                 proxies=None, num_retries=DEFAULT_RETRIES,
                 max_depth=DEFAULT_DEPTH,
                 timeout=DEFAULT_TIMEOUT, opener=None, cache=None):
        socket.setdefaulttimeout(timeout)
        self.throttle = Throttle(delay)
        self.user_agent = user_agent
        self.proxies = proxies
        self.num_retries = num_retries
        self.max_depth = DEFAULT_DEPTH
        self.opener = opener
        self.cache = cache
        self.js_regex = re.compile(Downloader.JS_REGEX, re.IGNORECASE)
        self.ws_regex = re.compile(Downloader.WS_REGEX, re.IGNORECASE)
        self.webpage_regex = re.compile(Downloader.WEB_REGEX, re.IGNORECASE)

    def __call__(self, seed_url, link_depth=3):
        self.crawl(seed_url, link_depth)
        return

    def crawl(self, url, depth):
        record = None
        if self.cache:
            try:
                record = self.cache[url]
            except KeyError:
                # url is not available in cache
                pass
            else:
                # if self.num_retries > 0 and 500 <= record['code'] < 600:
                    # server error so ignore result from cache and re-download
                    # result = None
                print('%s hits cache' % url)
        if record is None:
            self.throttle.wait(url)
            page = self.download_parse(url, depth)
            if depth == 0:
                return
            for link in self.get_webpage_links(url, page):
                self.crawl(link, depth-1)
        return

    def download_parse(self, url, depth):
        record = {}
        proxy = random.choice(self.proxies) if self.proxies else None
        headers = {'User-agent': self.user_agent}
        result = self.download(url, headers, proxy=proxy,
                               num_retries=self.num_retries)
        ws_urls = set()
        js_urls = set()
        html_urls = set()
        for ws_url in self.find_ws_links(url, result['html']):
            print("WS/WSS URL %s in HTML: %s" % (colored(ws_url, 'red'), url))
            ws_urls.add(ws_url)
            html_urls.add(url)
        for js_url in self.find_js_links(url, result['html']):
            print('Downloading JS: ' + colored(js_url, 'cyan'))
            js = self.download(js_url, headers, proxy=proxy,
                               num_retries=self.num_retries)
            for ws_url in self.find_ws_links(url, js['html']):
                print("WS/WSS URL %s in JS: %s" % (colored(ws_url, 'green'), js_url))
                ws_urls.add(ws_url)
                js_urls.add(js_url)
        print('PID %d saving %s info to DB...' % (os.getpid(), url))
        for ws in ws_urls:
            print(colored(ws, 'green'))
        if ws_urls:
            record['ws_urls'] = list(ws_urls)
            if html_urls:
                record['html_urls'] = list(html_urls)
            if js_urls:
                record['js_urls'] = list(js_urls)
            if self.cache:
            # save ws result to cache
                record['depth'] = self.max_depth - depth
                self.cache[url] = record
        return result['html']

    def get_webpage_links(self, url, html):
        """ Return a list of links from html
        """
        EXT = {".php", ".html", ".html", ".asp", ".aspx", ".jsp"}
        webpage_urls = set()
        for link in re.findall(self.webpage_regex, html):
            webpage_url = self.normalize(url, link)
            if self.same_domain(url, webpage_url):
                if self.get_ext(webpage_url).lower() in EXT:
                    webpage_urls.add(webpage_url)
        return list(webpage_urls) if webpage_urls else []


    def normalize(self, url, link):
        """Normalize this URL by removing hash and adding domain
        """
        link, _ = urlparse.urldefrag(link) # remove hash to avoid duplicates
        return urlparse.urljoin(url, link)

    def same_domain(self, url1, url2):
        """Return True if both URL's belong to same domain
        """
        return urlparse.urlparse(url1).netloc == urlparse.urlparse(url2).netloc

    def get_ext(self, url):
        """Return the filename extension from url, or ''."""
        parsed = urlparse.urlparse(url)
        root, ext = splitext(parsed.path)
        return ext  # or ext[1:] if you don't want the leading '.'

    def find_js_links(self, url, html):
        """Return a list of js links from HTML
        """
        print("PID %d Finding js links...." % os.getpid())
        links = re.findall(self.js_regex, html)
        js_urls = set()
        for link in links:
            js_link = self.normalize(url, link)
            if self.get_ext(js_link).lower() == '.js':
                js_urls.add(js_link)
        return list(js_urls) if js_urls else []

    def find_ws_links(self, url, html):
        """Return a list of ws/wss urls from HTML or JS
        """
        print("PID %d Finding ws links...." % os.getpid())
        ws_links = re.findall(self.ws_regex, html)
        ws_urls = set()
        for ws_link in ws_links:
            ws_urls.add(ws_link)
        return list(ws_urls) if ws_urls else []

    def download(self, url, headers, proxy, num_retries, data=None):
        print('PID %d Downloading %s:' % (os.getpid(), url))
        request = urllib2.Request(url, data, headers or {})
        opener = self.opener or urllib2.build_opener()
        if proxy:
            proxy_params = {urlparse.urlparse(url).scheme: proxy}
            opener.add_handler(urllib2.ProxyHandler(proxy_params))
        try:
            response = opener.open(request, timeout=10)
            html = response.read()
            code = response.code
        except Exception as e:
            print('Download error:', str(e), url)
            html = ''
            if hasattr(e, 'code'):
                code = e.code
                if num_retries > 0 and 500 <= code < 600:
                    # retry 5XX HTTP errors
                    return self.download(url, headers, proxy,
                                         num_retries-1, data)
            else:
                code = None
        return {'html': html, 'code': code}


class Throttle:
    """Throttle downloading by sleeping between requests to same domain
    """
    def __init__(self, delay):
        # amount of delay between downloads for each domain
        self.delay = delay
        # timestamp of when a domain was last accessed
        self.domains = {}

    def wait(self, url):
        """Delay if have accessed this domain recently
        """
        domain = urlparse.urlsplit(url).netloc
        last_accessed = self.domains.get(domain)
        if self.delay > 0 and last_accessed is not None:
            sleep_secs = self.delay - (datetime.now() - last_accessed).seconds
            if sleep_secs > 0:
                time.sleep(sleep_secs)
        self.domains[domain] = datetime.now()
