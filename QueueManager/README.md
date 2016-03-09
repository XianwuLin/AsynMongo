#QueueManager

QueueManager封装并扩展了python内置的Queue和Redis的队列，Redis队列改自hotqueue。并且提供了队列的http监控接口和web监控界面。

该队列仅实现的FIFO队列，队列常用方法均已实现。

队列方法如下：

1. Queue(name) #生成队列或返回已经存在的队列
2. Queue.qsize() #队列大小
3. Queue.put(obj) #入队
4. Queue.put_left(obj) #插入队列头部
5. Queue.get() #弹出队列元素
6. Queue.clear() #队列清空
7. Queue.key() #返回队列名称

队列管理者方法如下：

1. QueueManager() #获取队列管理对象，单例模式
2. QueueManager.Queue(queue_type="python_queue", name=None, **kwargs) #获取一个队列，队列名称为name, 队列类型为queue_type
3. QueueManager.pull_redis_queue(host="localhost", port=6379, **kwargs) #拉取对应redis下的队列
4. QueueManager.all_queues() #获取全部队列字典
5. QueueManager.remove(queue_object=None, name = None) #删除队列
6. QueueManager.qsize(name) #获取队列长度
7. QueueManager.shutdown() #关闭队列的监控
