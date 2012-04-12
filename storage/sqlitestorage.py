#!/usr/bin/env python
#-*- coding:utf-8 -*-!

import sqlite3
from threading import Thread
from Queue import Queue
from logger import *

class SqliteStorage(object):
    def __init__(self, name):
        conn = sqlite3.connect(name)
        try:
            conn.execute("""create table pages(url text unique, content text)""")
        except sqlite3.Error, e:
            LOGGER.warning('%s', str(e))
        conn.close()

        self.dbname = name
        self.queue = Queue(32) #limit size
        self.thread = Thread(target=self.worker_loop)
        self.thread.start()

    def save(self, url, content):
        self.queue.put((url,content))

    def worker_loop(self):
        LOGGER.debug('++++++++++ SQLite thread start! ++++++++++')
        conn = sqlite3.connect(self.dbname)
        while True:
            url, content = self.queue.get()
            if not url or not content:
                break
            try:
                LOGGER.debug('----Start to save %s, size %d', url, len(content))
                c = conn.cursor()
                c.execute('''insert into pages values (?, ?)''', [url, sqlite3.Binary(content)])
                conn.commit()
                c.close()
            except sqlite3.Error, e:
                LOGGER.error('%s', str(e))
            except Exception, e:
                LOGGER.critical('%s', str(e))
            finally:
                LOGGER.debug('----End saving %s', url)
                self.queue.task_done()
        conn.close()
        LOGGER.debug('++++++++++ SQLite thread end! ++++++++++')

    def join(self):
        self.queue.put((None,None))
        self.thread.join()
