#!/usr/bin/env python
# encoding: utf-8
# AUTHOR: XIANWU LIN
# EMAIL: linxianwusx@gmail.com
# TIME: 2016/3/4 14:30

'''
这个模块提供了一个OriginQueue类，扩展原生队列Queue。
提供了一个QueueManger类，管理队列，并提供http接口。
'''

from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
import urlparse
import threading
from Queue import Queue

try:
    import simplejson as json
except ImportError:
    import json

ip="127.0.0.1"
port=9999

class OriginQueue(Queue):
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

# 单例模式
def singleton(cls, *args, **kw):
    instances = {}
    def _singleton():
        if cls not in instances:
            instances[cls] = cls(*args, **kw)
        return instances[cls]
    return _singleton

#请求 对应队列的队列长度
#接口 /qsize?name=xxx  请求单个队列的长度
#接口 /all_qsize  请求全部队列的长度
#接口 / 请求监控首页
class HTTPHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.protocal_version = "HTTP/1.1"
        path = urlparse.urlparse(self.path)
        if path.path == '/qsize':
            query = path.query.split("=")
            if len(query) == 2:
                if query[0].strip() == "name":
                    pyq = Pyq()
                    size = pyq.qsize(query[1].strip())
                    self.send_response(200)
                    self.end_headers()
                    return_json = {
                        "name" : query[1].strip(),
                        "qsize" : str(size)
                        }
                    self.wfile.write(json.dumps(return_json))
        elif path.path == '/all_qsizes':
            pyq = Pyq()
            return_list = []
            for name in pyq.all_queues().keys():
                json1 = {
                    "name" :  name,
                    "qsize" : pyq.get_queue_size(name)
                }
                return_list.append(json1)
            self.send_response(200)
            self.end_headers()
            self.wfile.write(json.dumps(return_list))
        elif path.path == "/":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(open("index.html").read())
        else:
            self.send_response(400)
            self.end_headers()
            self.wfile.write("some wrong")

http_server = HTTPServer((ip, port), HTTPHandler)

def start_server():
    global http_server
    print "http://%s:%d/ is open." % (ip, port)
    http_server.serve_forever() #设置一直监听并接收请求

@singleton
class QueueManger(object):
    def __init__(self, bind_ip='127.0.0.1', bind_port=9988):
        self.bind_ip = bind_ip
        self.bind_port = bind_port
        self.queue_dict = dict()
        self.service_running = False
        if not self.service_running:
            self.t = threading.Thread(target=start_server,)
            self.t.start()

    def Queue(self, name = None, maxsize=0):
        if not name:
            name = "Queue%d" % len(self.queue_dict.items())
        queue = Origin_Queue(name, maxsize)
        self.queue_dict[name] = queue
        return queue

    def all_queues(self):
        return self.queue_dict

    def key(self, name):
        if self.queue_dict.has_key(name):
            return self.queue.key()
        else:
            return None

    def clear(self, name):
        if self.queue_dict.has_key(name):
            self.queue_dict.pop(name)
        else:
            raise Exception("No queue %s" % name)

    def qsize(self, name):
        if self.queue_dict.has_key(name):
            return self.queue_dict[name].qsize()
        else:
            raise Exception("No queue %s" % name)

    def shutdown(self):
        http_server.shutdown()

def main():
    import time
    QM = QueueManger()
    queue = QM.Queue(name="tt")
    queue1 = QM.Queue(name="qw")
    queue1.put("asdf")
    queue1.put(123)
    queue.put(123)
    queue.put(123)
    time.sleep(3)
    queue.put(34)
    time.sleep(1)
    queue.put(34)
    time.sleep(0.1)
    queue.get()
    queue.get()
    time.sleep(2)
    queue.get()
    time.sleep(15)
    QM.shutdown()

if __name__ == '__main__':
    main()
