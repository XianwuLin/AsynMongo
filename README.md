# AsynMongo

标签（空格分隔）： MongoDB python

---
Author: linxianwusx@gmail.com

AsynMongo是一个异步的操作MongoDB的python库，提供了以对象的方式进行操作MongoDB，内部使用了pymongo。

AsynMongo 提供了异步的插入和更新的方法，使用了一个单独的线程监控和执行异步队列，可以选择异步队列的实现，包括python_queue(python内置的Queue)和redis_queue(Redis队列)，关于队列的管理，自己实现了一个QueueManager的类，用来管理队列，并提供了web监控的方式，具体实现见QueueManager。

---

demo

```python
from AsynMongo import Collection
from pymongo import MongoClient


class Man():
    pass

def main():
    col = Collection(MongoClient().db.collection)

    man = Man()
    man.name ="bob"
    man.sex = "male"
    man.age = 20
    man.book_num = 5

    col.insert_asyn(man) #异步插入

    for item in col.find({"sex" : "male"}):
        if item.name = "bob":
            item.name = "lily"
            item.book_num += 1
            delattr(item,'age')

        col.update_asyn(item) #异步更新

    col.close() #等待队列执行完毕，关闭队列

if __name__ == "__main__":
    main()
```

程序执行完后并不会马上退出，需要异步更新，可以打开 http://127.0.0.1:9999 (默认)监控队列执行情况。

模块提供的方法有：

1. insert(object) #同步插入对象
2. insert_asyn(object) #异步插入对象
3. update(object) #同步更新对象
4. update_asyn(object) #异步更新对象
5. find() #查找，返回对象的generator
6. find_one() #查找一条
7. close() #异步不等待空队列，结束操作
8. qsize() #返回队列大小
9. set_collection(client, db, collection) #切换collection

方法尚不完善，正在后续更新。