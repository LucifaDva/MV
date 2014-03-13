#! /usr/bin/python

from worker import *
from task import *
import time
import datetime
import yaml
import sys
import logging
from twisted.internet import reactor, defer, protocol, threads
from twisted.web.client import HTTPDownloader, _parse
from twisted.python.failure import Failure
from twisted.python.util import println

conf_file = '../core/config.yaml'
config = yaml.load(file(conf_file))
logger = logging.getLogger('mylogger')

class DownloadWorker(Worker):
    def __init__(self, host, port, tube_name):
        Worker.__init__(self, host, port, tube_name)
        self.defer_count = 0
        self.count = 0

    def start(self):
        reactor.callInThread(self.do)
        reactor.run()

    def download(self, task):
        m = task.content
        url = str(m['url'])
        s, host, port, _= _parse(url)
        file_name = url.split('/')[-1]
        print 'start download: ' + url 
        factory = HTTPDownloader(url, '../apks/'+file_name)
        reactor.connectTCP(host, port, factory)
        def r(result):
            return task
        def err(failure):
            retries = task.release_count
            if retries > 3:
                #retries 3 times, create the err message
                task.err_str = str(failure)
                task.current_op = 'DOWNLOAD'
                task.next_op = 'ERROR'
                self.put_task(task, config['TUBE']['ALLOCATE'])
            else:
                task.release_count += 1
                self.put_task(task, config['TUBE']['DOWNLOAD'])
            assert self.defer_count > 0
            self.defer_count -= 1
   
        factory.deferred.addCallbacks(r, err)
        factory.deferred.addCallbacks(self.download_complete, self.download_fail)
        return factory.deferred

    def download_complete(self, task):
        if  isinstance(task, Task):
            self.save_result(task)
            assert self.defer_count > 0
            self.defer_count -= 1 

    def save_result(self, task):
        print 'save task# %d result' % (task.ID, )     
        
    def download_fail(self, failure):
        print 'failure***: ' , failure

    def do(self):
        while True:
            if self.defer_count < 3:
                task = self.get_task()
                print 'get new task'  
                self.defer_count += 1
                reactor.callFromThread(self.download,task)

def main():
    download_worker = DownloadWorker(config['SERVER_ADDRESS']['HOST'], config['SERVER_ADDRESS']['PORT'], config['TUBE']['DOWNLOAD'])
    download_worker.start()

if __name__ == '__main__':
    main()
