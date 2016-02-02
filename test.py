#!/usr/bin/env python
# -*- coding: utf-8 -*-
###############
# Date    : 2016-02-02 15:55:01
# Author  : Victor Lin
# Email    : linxianwusx@gmail.com
###############
from AsynMongo import Collection
from pymongo import MongoClient

class man():
    pass

def main():
    client = MongoClient("10.67.2.245")
    col = Collection(client.test.test)

    a = man()
    a.name = "bob"
    a.sex = "male"
    col.insert(a)

    print "finish!"

def main1():
    client = MongoClient("10.67.2.245")
    col = Collection(client.test.test)

    a = col.find_one({"name" : "bob", "sex" : "male"})
    a.sex = "female"

    print a.__dict__

    col.update(a)

if __name__ == "__main__":
    main1()