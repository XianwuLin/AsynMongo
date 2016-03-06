
class BQ():
    def __init__(self, qtype = 'Queue', qname = None, **k):
        if qtype == "Queue":
            from python-queue import QueueManger
            self.queue_manger = QueueManger()
            self.queue = self.queue_manger.Queue(name = qname, **k)
        elif qtype == "Redis":
            from redisQ import RedisQ
            self.queue = RedisQ(name = qname, **k)

    def get(self, *a, **k):
        return self.queue.get(*a, **k)

    def put(self, *a, **k):
        return self.queue.put(*a, **k)

    def put_left(self, *a, **k):
        return self.queue.put_left(*a, **k)

    def qsize(self, *a, **k):
        return self.queue.qsize(*a, **k)

    def clear(self, *a, **k):
        return self.queue.clear(*a, **k)

    def key(self):
        return self.key()

    # def keys(self):
    #     return self.keys()
