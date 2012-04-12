#!/usr/bin/env python
#-*- coding:utf-8 -*-

def valid_filename(fname):
    n = list()
    for c in fname:
        if c.isalnum() or c not in ':?*"/\|':
            n.append(c)
        else:
            n.append('%'+c.encode('hex'))
    return ''.join(n)[:248]
