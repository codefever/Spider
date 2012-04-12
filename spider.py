#!/usr/bin/env python
#-*- coding:utf-8 -*-

import sys
import urllib2
from Queue import Queue
from bs4 import BeautifulSoup, Doctype, Comment
from time import sleep,time

#Custom Modules
from logger import *
from storage import *
from threadpool import *
from status import *
from defaults import *

class Spider(object):
    def __init__(self, url, max_depth=10000, workers=2, storage=None, keyword=None):
        self.url = url
        if self.url[-1] <> '/':
            self.url += '/'
        self.max_depth = max_depth

        self.stat = Status()
        self.storage = storage      #sth for storing pages
        self.parsed = set()         #urls processed

        #Set up keyword searching function
        if keyword:
            LOGGER.debug('Search keyword: %s', keyword)
            self.keyword = keyword.decode(INPUT_ENCODING)
        else:
            LOGGER.debug('No search keyword')
            self.keyword = None

        #Single-threading
        #self.queue = Queue()
        #self.queue.put((self.url, 0))
        
        #Worker pool
        self.pool = ThreadPool(workers)
        self.pool_stop = True

    def run(self):
        """Worker pool mode"""
        self.pool_stop = False
        self.pool.start()
        self.stat.clear()

        #add 1st job
        self.pool.add_job(self.worker, self.url, 0)

        last_output = time()
        idle_count = 0  #To judget if it's really idle

        try:
            LOGGER.info('++++++++++ Run! ++++++++++')
            while True:
                now = time()
                #IDLE judgement
                if self.pool.idle():
                    idle_count += 1
                    if idle_count >= IDLE_TO_QUIT:
                        break
                else:
                    idle_count = 0
                #Should show stat?
                if now - last_output >= OUTPUT_INTERVAL:
                    self.print_stat()
                    last_output = now
                sleep(2)
        except KeyboardInterrupt:
            LOGGER.info('++++++++++ [CTRL-c] received! ++++++++++')
        finally:
            self.stop()
            print '\n\nFinal stat:'
            self.print_stat()
            LOGGER.info('++++++++++ Terminated! ++++++++++')

    def stop(self):
        self.pool_stop = True
        self.pool.stop(immediately=True)

        #UGLY: wait for storage
        if self.storage and hasattr(self.storage, 'join') and callable(self.storage.join):
            self.storage.join()

    def worker(self, url, depth):
        LOGGER.debug('Start with %s', url)
        content, urls, match = self.fetch(url, fetch_urls=(depth+1 < self.max_depth), keyword=self.keyword)
        self.parsed.add(url) #Mark parsed
        if not content:
            self.stat.update(failed=1)
            return
        #Save page
        if match and self.storage:
            self.storage.save(url, content)
        #Handle child urls
        for u in urls:
            if u and u not in self.parsed:
                self.pool.add_job(self.worker, u, depth+1)
        self.stat.update(matched=match)
        LOGGER.debug('End with %s', url)

    def bs4_search_keyword(self, soup, keyword):
        """
        Use BeautifulSoup object to search.
        It's better to put these all in a new module.
        """
        def text_filter(elem):
            if isinstance(elem, (Doctype, Comment)):
                return False
            elif elem.parent and elem.parent.name in ['script', 'noscript', 'link']:
                return False
            else:
                return True
        text = soup.find_all(text=text_filter)
        for t in text:
            if keyword in unicode(t).strip():
                return True
        return False

    def fetch(self, url, fetch_urls=False, keyword=None):
        LOGGER.info('-- Fetch %s', url)
        urls = list()
        content = None
        match = False
        try:
            content = urllib2.urlopen(url, timeout=FETCH_TIMEOUT).read()
        except urllib2.URLError, e:
            LOGGER.error('%s %s', str(e), url)
            return (content, urls, match)
        except Exception, e:
            LOGGER.error('%s %s', str(e), url)
            return (content, urls, match)
        soup = BeautifulSoup(content, 'lxml')
        #Search keyword
        if keyword:
            match = self.bs4_search_keyword(soup, keyword)
        #Check if we should search deeper or not.
        if fetch_urls:
            for a in soup.find_all('a'):
                if a.get('href'):
                    u = self.normalized_url(url, a['href'])
                    if u:
                        urls.append(u)
        return (content, urls, match)

    def normalized_url(self, base_url, url):
        u = urllib2.urlparse.urlsplit(url)
        if not u.scheme and not u.netloc:
            #Relative URL.
            return urllib2.urlparse.urljoin(base_url, u.path)
        else:
            #Absolute URL.
            if u.scheme in ['http', 'https', 'ftp']:
                return url

    def print_stat(self):
        st = self.stat.get()
        qsize = self.pool.jobs_count()
        print 'Total: %d,  Fetch-failed: %d,  Matched: %d,  In-Queue: %d' %(st['total'], st['failed'], st['matched'], qsize)

def load_options(args):
    from optparse import OptionParser

    parser = OptionParser('usage: %prog -u URL -d DEPTH [options]')
    parser.add_option('-u', dest='url', type='string', help='isinstance(text[0],(Doctype,Comment))URL to start with.', metavar='URL') #URL option
    parser.add_option('-d', dest='depth', type='int', help='Parse depth.', metavar='DEPTH') #DEPTH option
    parser.add_option('-l', dest='loglevel', default=3, type='int', help='Log level.', metavar='LOGLEVEL') #LOGLEVEL option
    parser.add_option('-f', dest='logfile', type='string', help='Log file name.', metavar='LOGFILE') #LOGFILE option
    parser.add_option('', '--thread', dest='thread', type='int', default=10, help='Thread number.', metavar='THREAD') #THREAD option
    parser.add_option('', '--dbfile', dest='dbfile', type='string', help='SQLite database file name.', metavar='DBFILE') #DBFILE option
    parser.add_option('', '--key', dest='keyword', type='string', help='Search keyword.', metavar='KEYWORD') #KEYWORD option
    (options, unknown) = parser.parse_args(args)

    for un in unknown[1:]:
        print '[UNKNOWN] %s' % un

    #URL
    if not options.url:
        raise ValueError('URL is required.')
    #DEPTH
    if not options.depth:
        raise ValueError('DEPTH is required.')
    #THREAD
    if options.thread < 1:
        raise ValueError('THREAD >= 1.')

    #Logging
    init_logger(options.loglevel, options.logfile)

    return options

if __name__=='__main__':
    opts = load_options(sys.argv)
    if opts.dbfile:
        store = SqliteStorage(opts.dbfile)
    elif FILE_STORE_DIR:
        store = FileStorage(FILE_STORE_DIR)
    else:
        store = None

    spider = Spider(url=opts.url, max_depth=opts.depth, workers=opts.thread, storage=store, keyword=opts.keyword)
    spider.run()
