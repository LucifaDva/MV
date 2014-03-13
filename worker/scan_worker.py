#! /usr/bin/python

from worker import *
import time
import yaml
import sys

conf_file = '../core/config.yaml'
config = yaml.load(file(conf_file))

class ScanWorker(Worker):
    def __init__(self, host, port, tube_name):
        Worker.__init__(self, host, port, tube_name)

    def start(self):
        while(True):
            task = self.get_content(self.get_task())
            self.do(task)
            
    def do(self, task):
        time.sleep(2)
        task.current_op = 'SCAN'
        task.next_op = 'FINISH'
        self.put_task(task,config['TUBE']['ALLOCATE'])

def main():
    scan_worker = ScanWorker(config['SERVER_ADDRESS']['HOST'], config['SERVER_ADDRESS']['PORT'], config['TUBE']['SCAN'])
    scan_worker.start()

if __name__ == '__main__':
    main()
