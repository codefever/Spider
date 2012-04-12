#!/usr/bin/env python
#-*- coding:utf-8 -*-

__all__ = ['Status']

import threading

class Status(object):
    def __init__(self):
        self.total_fetched = 0
        self.fetch_failed = 0
        self.fetch_matched = 0
        self.lock = threading.Lock()

    def update(self, failed=0, matched=0):
        self.lock.acquire()
        self.total_fetched += 1
        if failed:
            self.fetch_failed += 1
        if matched:
            self.fetch_matched += 1
        self.lock.release()

    def clear(self):
        self.lock.acquire()
        self.total_fetched = 0
        self.fetch_failed = 0
        self.fetch_matched = 0
        self.lock.release()

    def get(self):
        ret = {}
        self.lock.acquire()
        ret['total'] = self.total_fetched
        ret['failed'] = self.fetch_failed
        ret['matched'] = self.fetch_matched
        self.lock.release()
        return ret
