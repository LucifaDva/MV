#! /usr/bin/python

import beanstalkc
from task import *

class Worker(object):
    def __init__(self, host, port, tube_name):
        self.beanstalk = beanstalkc.Connection(host, port)
        self.beanstalk.watch(tube_name)
        self.beanstalk.ignore('default')

    def get_task(self):
        job = self.beanstalk.reserve()
        task = self.get_content(job)
        job.delete()
        return task

    def put_task(self, task, tube_name):
        self.beanstalk.use(tube_name)
        self.beanstalk.put(task.to_string())

    def get_content(self, task):
        print 'get message: %s' % (task.body,)
        t = Task()
        t.to_job(task.body)
        return t

