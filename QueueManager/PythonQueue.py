#!/usr/bin/env python
# -*- coding: utf-8 -*-
###############
# Date    : 2016-03-08 10:32:06
# Author  : Victor Lin
# Email    : linxianwusx@gmail.com
###############
from Queue import Queue

class PythonQueue(Queue):
    def __init__(self, name, maxsize = 0):
        Queue.__init__(self, name)
        self.name = name

    def put_left(self,item):
        self.queue.appendleft(item)

    def clear(self):
        self.queue.clear()
        return True

    def key(self):
        return self.name