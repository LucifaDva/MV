#! /usr/bin/python

from task import *
from worker import *
import beanstalkc
import yaml
import sys
import csv
import logging

conf_file = '../core/config.yaml'
config = yaml.load(file(conf_file))
logger = logging.getLogger('mylogger')

class JobAllocate(Worker):
    def __init__(self, host, port, tube_name):
        Worker.__init__(self, host, port, tube_name)

    def start(self):
        while True:
            task = self.get_task()
            self.allocate_task(task)
             
    def allocate_task(self, task):
        task.current_op = 'ALLOCATE'
        if task.next_op == 'DOWNLOAD':
            self.put_task(task, config['TUBE']['DOWNLOAD'])
        elif task.next_op == 'VERIFY':
            self.put_task(task, config['TUBE']['URLVERIFY'])
        elif task.next_op == 'BATCH_DOWNLOAD':
            self.decompose_task(task)
        elif task.next_op == 'SCAN':
            print 'task %d end %s start %s' % (task.ID, task.current_op, task.next_op)
            self.put_task(task, config['TUBE']['SCAN'])
        elif task.next_op == 'FINISH':
            print 'task %d finished' % (task.ID, )
        elif task.next_op == 'ERROR':
            print 'task %d error: %s' % (task.ID, task.err_str)

    def decompose_task(self, task):
        file_path = str(task.content)
        logger.debug('file path: ' + file_path)
        reader = csv.reader(open(file_path))
        for line in reader:
            print 'new sub task'
            if len(line) == 0:
                continue
            t = Task()
            t.ID = reader.line_num
            md5 = (line[5]).decode('gb2312')
            url = (line[6]).decode('gb2312')
            t.content = {'md5': md5, 'url': url}
            t.current_op = 'ALLOCATE'
            t.next_op = 'DOWNLOAD'
            self.put_task(t, config['TUBE']['DOWNLOAD'])

    def close(self):
        self.beanstalk.close()
       
def main():
    job_allocate = JobAllocate(config['SERVER_ADDRESS']['HOST'], config['SERVER_ADDRESS']['PORT'], config['TUBE']['ALLOCATE'])
    job_allocate.start()

if __name__ == '__main__':
    main()
