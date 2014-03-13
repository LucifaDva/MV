#! /usr/bin python

import json

class Task(object):
    def __init__(self, ID = None, content = None, next_op = None, priority = 8388607):
        self.ID = ID
        self.content = content
        self.next_op = next_op
        self.current_op = None
        self.err_str = None
        self.priority = priority
        self.release_count = 0

    def to_string(self):
        m = {'ID' : self.ID,
             'content' : self.content,
             'next_op' : self.next_op,
             'err_str' : self.err_str,
             'current_op' : self.current_op,
             'release_count': self.release_count,
             'priority' : self.priority}
        return json.dumps(m)

    def to_job(self, str_job):
        m = json.loads(str_job)
        self.ID = m['ID']  
        self.content = m['content']
        self.next_op = m['next_op']
        self.current_op = m['current_op']
        self.err_str = m['err_str']
        self.priority = m['priority']
        self.release_count = m['release_count']

class WaitTask(object):
    def __init__(self, task_ID, sub_task_count, finish_task_count, err_task_count):
        self.task_ID = task_ID
        self.sub_task_count = sub_task_count
        self.finish_task_count = finish_task_count
        self.err_task_count = err_task_count

    def add_finish(self):
        if self.finish_task_count + self.err_task_count >= self.sub_task_count:
            print 'task #%d: all sub task has finished' % (self.task_ID, )
            return 0
        else:
            self.finish_task_count += 1
            return 1

    def add_error(self):
        if self.finish_task_count + self.err_task_count >= self.sub_task_count:
            print 'task #%d: all sub task has finished' % (self.task_ID, )
            return 0
        else:
            self.err_task_count += 1
            return 1


