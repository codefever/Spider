#!/usr/bin/env python
#-*- coding:utf-8 -*-!

from utils import *
from logger import *

#Print out for test.
class FileStorage(object):
    def __init__(self, dir):
        if dir[-1] <> '/':
            dir += '/'
        self.dir = dir

    def save(self, url, content):
        name = url.replace(':', '%'+':'.encode('hex')).replace('/', '%'+'/'.encode('hex'))
        try:
            with open(self.dir + valid_filename(name) + '.txt', 'w') as f:
                f.write(content)
        except Exception, e:
            LOGGER.error('%s', str(e))
