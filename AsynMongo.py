#!/usr/bin/env python
# -*- coding: utf-8 -*-
###############
# Date    : 2016-01-11 12:43:43
# Author  : Victor Lin
# Email   : linxianwusx@gmail.com
# Website : https://github.com/XianwuLin/AsynMongo
# Version : 0.2.5
###############

"""
这个模块封装了pymongo，提供了插入、查询、更新，以对象的形式进行操作，提供了异步插入和更新的方法。

注意：不要在MongoDB中使用 _origin 这个字段
demo：

from pymongo import MongoClient
from AsynMongo import Collection

class man():
    pass

#初始化
client = MongoClient()
col = Collection(client.test.test)

#插入
item = man()
item.name = "bob"
item.sex = "male"
col.insert(item) #同步
col.insert_asyn(item) #异步

#更新
a = col.find_one({"name" : "bob", "sex" : "male"})
#for item in col.find({}):
    #do something
a.sex = "female"
col.update(item) #同步
col.update_asyn(item) #异步

#等待异步操作完成
col.close()

"""

from pymongo import MongoClient
from Queue import Queue, Empty
from copy import deepcopy
import threading
import time

__version__ = "0.2.4"
#Queue 模块补丁
def put_left(self,item):
    self.queue.appendleft(item)

Queue.put_left = put_left


# 单例模式
class Borg(object):
    _state = {}

    def __new__(cls, *args, **kw):
        ob = super(Borg, cls).__new__(cls, *args, **kw)
        ob.__dict__ = cls._state
        return ob


#字典打包对象
class obj(object):
    def __init__(self, **entries):
        self.__dict__.update(entries)
        self._origin = deepcopy(self.__dict__)


class Collection(object):
    def __init__(self, collection):
        self.collection = collection
        self.initialize()

    def initialize(self):
        self.queue = Queue()
        self.asyn_collection = None
        self.runable = False
        self.timeout = 60
        self.l_list = []  #插入任务
        self.u_list = []  #更新任务

    @staticmethod
    def _get_dict(ob):
        if not hasattr(ob,"_origin"):
            return ob.__dict__
        else:
            dict1 = dict(ob.__dict__)
            dict1.pop("_origin")
            return dict1

    @staticmethod
    def _get_remove_dict(dict_origin, dict_now):
        remove_list = list(set(dict_origin.keys()).difference(dict_now.keys()))
        remove_dict = dict()
        for i in remove_list:
            remove_dict[i] = 1
        return remove_dict

    @staticmethod
    def _get_update_dict(dict_origin, dict_now):
        update_dict = dict()

        for item in dict_now.items():
            k,v = item
            if dict_origin.has_key(k) and dict_origin[k] == v:
                pass
            else:
                update_dict[k] = v

        return update_dict

    def set_collection(self, client, db, collection):
        self.collection = client.get_database(db).get_collection(collection)

    def qsize(self):
        return self.queue.qsize()

    def insert(self, ob):  # 同步插入
        self.collection.insert_one(self._get_dict(ob))

    def insert_asyn(self, ob, lsize=50, timeout=60):  # 异步插入
        if not self.runable:
            self.run(lsize=lsize)

        self.timeout = timeout
        self.queue.put([self.collection, "insert", ob])

    def update(self, ob):  # 更新
        if not hasattr(ob, "_id"):
            raise Exception("not a normal mongo item")
        self._real_update(ob)

    def update_asyn(self, ob, lsize=50, timeout=60):  # 异步更新
        if not self.runable:
            self.run(lsize=lsize)

        self.timeout = timeout
        self.queue.put([self.collection, "update", ob])

    def find(self, json = dict(), item= dict(), limit=0, skip=0):  # 查询，返回对象generator
        if not limit:
            if not item:
                result = self.collection.find(json).skip(skip)
            else:
                result = self.collection.find(json, item).skip(skip)
        else:
            if not item:
                result = self.collection.find(json).skip(skip).limit(limit)
            else:
                result = self.collection.find(json, item).skip(skip).limit(limit)
        if not result:
            yield None
        else:
            for item in result:
                yield obj(**item)

    def find_one(self,  json = dict(), item= dict()):  # 查询一条，返回对象
        if not item:
            result = self.collection.find_one(json)
        else:
            result = self.collection.find_one(json, item)
        if not result:
            return None
        else:
            return obj(**result)

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
            self.queue.put("X")
            if self.t:
                self.t.join()
                return
            else:
                return
        else:
            return

    def _run_last(self): # 执行上个循环任务
        if self.l_list:  # 插入
            self._real_insert_asyn(self.l_list)
            self.l_list = []
        else:
            pass

        if self.u_list: # 更新
            for ob in self.u_list:
                self._real_update(ob)
        else:
            pass

    def _get_size(self):
        if self.queue.qsize() > self.lsize:  # 获取这个队列中的大小
            size = self.lsize
        else:
            if self.queue.qsize():
                size = self.queue.qsize()
            else:
                size = 1  # 如果队列为空，只要下次有数据插入（队列大于一），就会被捕获，激活线程
        return size

    def _run_single(self):
        while self.runable:
            self._run_last()
            size = self._get_size()

            for i in xrange(size):
                try:
                    item = self.queue.get(timeout=self.timeout)

                    if isinstance(item, str):
                        if item == "X":  # "X"为停止信号
                            self.runable = False
                            break

                    elif isinstance(item, list):
                        collection, mark, ob = item   #第一位为集合，第二位为标志位，第三位为对象

                        if i == 0: #第一次运行循环，设置异步collection
                            self.asyn_collection = collection

                        if self.asyn_collection == collection:  #保证一个循环的collection是相同的
                            if mark == "insert":
                                self.l_list.append(self._get_dict(ob))
                            elif mark == "update":
                                self.u_list.append(ob)
                        else:  #如果不同，把元素放回去
                            self.queue.put_left(item)
                            break
                    else:
                        raise Exception("Error Queue message:\t" + item)
                except Empty:
                    self.runable = False
                    break

    def _real_insert_asyn(self, l_list):
        self.collection.insert_many(l_list)

    def _real_update(self, ob):
        if not hasattr(ob, "_id"):
            raise Exception("not a normal mongo item")
        elif not hasattr(ob, "_origin"):
            raise Exception("have no _origin data, can't use update_asyn callable")

        dict_now = self._get_dict(ob)
        dict_origin = ob._origin
        update_dict = self._get_update_dict(dict_origin, dict_now) #要$set的字典
        remove_dict = self._get_remove_dict(dict_origin, dict_now) #要$unset的字典

        self.collection.update({"_id": ob._id}, {"$set": update_dict, "$unset" : remove_dict})

###################
##测试用例
###################
class man():
    def __init__(self, i=""):
        self.name = "bob" + str(i)
        self.sex = "man"


def main():
    client = MongoClient("localhost",27017)
    col = Collection(client.test.woman)

    for item in col.find({"name" : {"$exists" : True}}):
        break

    # 同步插入对象
    col.insert(man())
    print "asyn insert ok!"

    # 查询
    dict1 = {"name" : "bob"}
    a = col.find(dict1)
    for item in col.find(dict1):
        # print item.name
        pass

    #同步更新
    dict2 = {"sex": "man"}
    item = col.find_one(dict2)
    if item:
        item.name = "lily"
        item.sex = "woman"
        col.update(item)
    else:
        pass

    print "sync insert ok!"


    # 异步插入对象
    for i in xrange(1000):
        col.insert_asyn(man(), lsize=200, timeout=5)  # 插入对象, lsize为粒度大小, 默认50; 空队列等待5s, 默认60s
    print "sync insert ok!"

    col.set_collection(client = client, db = "test", collection= 'woman')  # 申明数据库， ip, 端口，col名称，collection名称
    #异步更新
    dict2 = {"sex": "man"}
    for item in col.find(dict2):
        if item:
            item.name = "lily"
            item.sex = "woman"
            col.update_asyn(item)
        else:
            pass

    print "asyn update ok!"

    col.close() #异步不等待空队列，直接关闭


if __name__ == "__main__":
    main()
