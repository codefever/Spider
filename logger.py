#!/usr/bin/env python
#-*- coding:utf-8 -*-

__all__ = ['LOGGER', 'init_logger']

import logging
from defaults import *

LOGGER = logging.getLogger('spider')

LOG_LEVEL_TABLE = {
        0:logging.DEBUG,
        1:logging.INFO,
        2:logging.WARNING,
        3:logging.ERROR,
        4:logging.CRITICAL,
        5:logging.FATAL,
    }

LOG_FMT = '%(asctime)s %(name)s[%(levelname)s]: %(message)s'

def init_logger(level, filename):
    if level not in LOG_LEVEL_TABLE:
        raise ValueError('LOGLEVEL is invalid.')
    if not filename:
        filename = LOG_FILE_NAME
    LOGGER.setLevel(logging.DEBUG)
    fmt = logging.Formatter(LOG_FMT)
    #To console
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    ch.setLevel(LOG_LEVEL_TABLE[LOG_LEVEL_CONSOLE])
    #To file
    fh = logging.FileHandler(filename)
    fh.setFormatter(fmt)
    fh.setLevel(LOG_LEVEL_TABLE[level])

    LOGGER.addHandler(ch)
    LOGGER.addHandler(fh)
