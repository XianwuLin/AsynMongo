#!/usr/bin/env python
# -*- coding: utf-8 -*-
###############
# Date    : 2016-01-11 12:43:43
# Author  : Victor Lin
# Email   : linxianwusx@gmail.com
# Version : 0.2
###############
"""
这个模块封装了pymongo，提供了插入、查询、更新，以对象的形式进行操作，提供了异步插入和更新的方法。
"""

from pymongo import MongoClient
import Queue
import threading
import time


class Borg(object):  # 单例模式
    _state = {}

    def __new__(cls, *args, **kw):
        ob = super(Borg, cls).__new__(cls, *args, **kw)
        ob.__dict__ = cls._state
        return ob


class obj(object):
    def __init__(self, d):
        for a, b in d.items():
            if isinstance(b, (list, tuple)):
                setattr(self, a, [obj(x) if isinstance(x, dict) else x for x in b])
            else:
                setattr(self, a, obj(b) if isinstance(b, dict) else b)


class Db(Borg):
    def __init__(self, ip, port, db, collection):
        self.client = MongoClient(ip, port)
        self.db_str = db
        self.collection_str = collection
        self.collection = self.client.get_database(self.db_str).get_collection(self.collection_str)
        self.queue = Queue.Queue()
        self.runable = False
        self.timeout = 60
        self.l_list = []  #插入任务
        self.u_list = []  #更新任务

    def collection(self,collection = ""): #返回pymongo原生collection对象或设置collection
        if collection:
            self.collection_str = collection
            self.collection = self.client.get_database(self.db_str).get_collection(self.collection_str)
        return self.collection
        

    def insert(self, ob):  # 同步插入
        self.collection.insert_one(vars(ob))

    def insert_asyn(self, ob, lsize=50, timeout=60):  # 异步插入
        if not self.runable:
            self.run(lsize=lsize)

        self.timeout = timeout
        self.queue.put(["insert", ob])

    def update(self, ob):  # 更新
        if not hasattr(ob, "_id"):
            raise Exception("not a normal mongo item")

        self.collection.replace_one({"_id": ob._id}, vars(ob))

    def update_asyn(self, ob, lsize=50, timeout=60):  # 异步更新
        if not self.runable:
            self.run(lsize=lsize)

        self.timeout = timeout
        self.queue.put(["update", ob])

    def find(self, json, limit=0, skip=0):  # 查询，返回对象generator
        if not limit:
            result = self.collection.find(json).skip(skip)
        else:
            result = self.collection.find(json).skip(skip).limit(limit)
        if not result:
            yield obj({})
        else:
            for item in result:
                yield obj(item)

    def find_one(self, json):  # 查询一条，返回对象
        result = self.collection.find_one(json)
        if not result:
            return None
        else:
            return obj(result)

    def run(self, lsize=50):
        while self.queue.qsize():
            _ = self.queue.get()
        self.runable = True
        self.lsize = lsize
        self.t = threading.Thread(target=self._run_single)
        self.t.start()

    def close(self):
        while self.queue.qsize():  # 等待未完成任务
            time.sleep(0.2)
        if self.runable:
            self.queue.put_nowait("X")
            if self.t:
                self.t.join()
                return
            else:
                return
        else:
            return

    def _run_single(self):
        while self.runable:
            if self.l_list:  # 执行上个循环的插入异步任务
                self._real_insert_asyn(self.l_list)
                self.l_list = []
            else:
                pass

            if self.u_list: # 执行上个循环的更新异步任务
                for ob in self.u_list:
                    self.update(ob)
            else:
                pass

            if self.queue.qsize() > self.lsize:  # 获取这个队列中的大小
                size = self.lsize
            else:
                if self.queue.qsize():
                    size = self.queue.qsize()
                else:
                    size = 1  # 如果队列为空，只要下次有数据插入（队列大于一），就会被捕获，激活线程

            for i in xrange(size):
                try:
                    item = self.queue.get(timeout=self.timeout)
                    if isinstance(item, str):
                        if item == "X":  # "X"为停止信号
                            self.runable = False
                            break
                    elif isinstance(item,list):
                        mark, ob = item   #第一位为标志位，第二位为对象
                        if mark == "insert":
                            self.l_list.append(vars(ob))
                        elif mark == "update":
                            self.u_list.append(ob)
                    else:
                        raise Exception("Error Queue message:\t" + item)
                except Queue.Empty:
                    self.runable = False
                    break

    def _real_insert_asyn(self, l_list):
        self.collection.insert_many(l_list)


###################
##测试用例
###################
class man():
    def __init__(self, i=""):
        self.name = "bob" + str(i)
        self.sex = "man"


def main():
    db = Db("127.0.0.1", 27017, "test", 'man')  # 申明数据库， ip, 端口，db名称，collection名称

    #pymongo原生的collection对象
    col = db.collection
    for item in col.find({"name" : {"$exists" : True}}):
        print item["name"]
        print item["sex"]
        break

    # 同步插入对象
    db.insert(man())

    # 查询
    dict1 = {"name" : "bob"}
    a = db.find(dict1)
    for item in db.find(dict1):
        print item.name

    #同步更新
    dict2 = {"sex": "man"}
    item = db.find_one(dict2)
    if item:
        item.name = "lily"
        item.sex = "woman"
        db.update(item)
        print "OK"
    else:
        print "None"


    # 异步插入对象
    for i in xrange(1000):
        db.insert_asyn(man(), lsize=200, timeout=5)  # 插入对象, lsize为粒度大小, 默认50; 空队列等待5s, 默认60s


    #异步更新
    dict2 = {"sex": "man"}
    for item in db.find(dict2):
        if item:
            item.name = "lily"
            item.sex = "woman"
            db.update_asyn(item)
            print "OK"
        else:
            print "None"

    db.close() #异步不等待空队列，直接关闭


if __name__ == "__main__":
    main()
