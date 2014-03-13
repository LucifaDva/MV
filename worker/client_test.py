#! /usr/bin/python


from worker import *
import beanstalkc
import yaml
import sys

conf_file = '../core/config.yaml'
config = yaml.load(file(conf_file))

beanstalk = beanstalkc.Connection(config['SERVER_ADDRESS']['HOST'], config['SERVER_ADDRESS']['PORT'])
beanstalk.use(config['TUBE']['ALLOCATE'])
"""
urls = ['//gdown.baidu.com/data/wisegame/4cfde1464fbb77c4/yangshixinwen_512.apk',
        'http://gdown.baidu.com/data/wisegame/b95989d1e9a8e036/fangzhengziku_9.apk',
        '//gdown.baidu.com/data/wisegame/f0ba041578fa2588/caimaocaipiao_51.apk']
"""

task = Task()
task.ID = 1
task.current_op = 'CLIENT'
task.next_op = 'BATCH_DOWNLOAD'
task.content = '../worker/app_store.csv' 
beanstalk.put(task.to_string())
