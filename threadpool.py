#!/usr/bin/env python
#-*- coding:utf-8 -*-

__all__ = ['ThreadPool']

from Queue import Queue
import threading
from logger import *

class WorkerExit(Exception):
    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)

def worker_exit():
    raise WorkerExit('worker exit')

def worker_loop(pool):
    while pool.worker_run:
        job = pool.jobs.get()
        pool.mark_busy(1)
        try:
            job[0](*job[1], **job[2])
        except WorkerExit, e:
            #Signal for thread-exit
            break
        except Exception, e:
            #All exceptions stopped here.
            LOGGER.error('Thread Pool: %s', str(e))
        except:
            LOGGER.error('Thread Pool: unknown error')
        finally:
            pool.mark_busy(-1)
            pool.jobs.task_done()

class ThreadPool(object):
    def __init__(self, num):
        self.num_of_workers = num
        self.workers = list()
        self.jobs = Queue()
        self.allow_jobs = False
        self.worker_run = False

        #To tell whether is all idle
        self.busy_num = 0
        self.busy_lock = threading.Lock()

    def start(self):
        self.allow_jobs = True
        self.worker_run = True
        for i in xrange(self.num_of_workers):
            t = threading.Thread(target=worker_loop, args=(self,))
            t.start()
            self.workers.append(t)

    def stop(self, immediately=False):
        self.allow_jobs = False
        if immediately:
            #Workers will quit when they see the flag is changed.
            self.worker_run = False
        #Add jobs here for ensuring workers are all awaken.
        for i in xrange(self.num_of_workers):
            self.add_job(worker_exit)
        while len(self.workers) > 0:
            t = self.workers.pop()
            t.join()

    def add_job(self, func, *args, **kwargs):
        if self.allow_jobs and callable(func) or func == worker_exit:
            self.jobs.put((func, args, kwargs))

    def mark_busy(self, b):
        self.busy_lock.acquire()
        self.busy_num += b
        self.busy_lock.release()

    def busy(self):
        self.busy_lock.acquire()
        b = self.busy_num
        self.busy_lock.release()
        return b

    def idle(self):
        self.busy_lock.acquire()
        b = self.busy_num
        self.busy_lock.release()
        return (b == 0) and self.jobs.empty()

    def jobs_count(self):
        return self.jobs.qsize()

if __name__=='__main__':
    def worker():
        print '123'

    tp = ThreadPool(3)
    tp.start()

    for i in range(20):
        tp.add_job(worker)

    tp.stop()
