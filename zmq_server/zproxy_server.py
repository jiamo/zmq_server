#!/usr/bin/python
# -*- coding:utf-8 -*-

import argparse
import logging
import multiprocessing
import os
import signal
import sys
import time
import traceback
from enum import Enum

import msgpack
import zmq
from zmq.eventloop import ioloop, zmqstream


class ProcessingState(Enum):
    WAIT_FOR_MSG = 0
    IN_MSG = 1


class ZmqProcess(multiprocessing.Process):
    """ zmq base process """

    def __init__(self):
        super(ZmqProcess, self).__init__()

        self.context = None
        self.loop = None

    def setup(self):
        self.loop = ioloop.IOLoop.instance()
        self.context = zmq.Context()

    def stream(self, sock_type, addr, bind, callback=None, subscribe=b''):
        sock = self.context.socket(sock_type)
        if bind:
            sock.bind(addr)
        else:
            sock.connect(addr)

        # Add a default subscription for SUB sockets
        if sock_type == zmq.SUB:
            sock.setsockopt(zmq.SUBSCRIBE, subscribe)

        # Create the stream and add the callback
        stream = zmqstream.ZMQStream(sock, self.loop)
        if callback:
            stream.on_recv(callback)

        return stream


class MessageHandler(object):
    """ backend frontend handler"""

    def __init__(self):
        pass

    def __call__(self, msg):
        pass


class WorkerManager(object):

    def __init__(self):
        self.workers = []
        self.available_workers = 0
        self.available_workerList = []
        self.workerPids = []
        self.clientWorkerMap = {}  # when one finish the clientWorkerMap should pop it

    def KillWorkers(self):
        pass


class ClientendHandler(MessageHandler):
    """
    handle msg from client and  user backend send to worker
    """

    def __init__(self, worker_backend, client_frontend, workerMangaer, stop,
            logHandler):

        super(ClientendHandler, self).__init__()
        self.worker_backend = worker_backend
        self.client_frontend = client_frontend
        self._stop = stop
        self.workerManager = workerMangaer
        self.logHandler = logHandler
        self.logger = logging.getLogger("ClientendHandler")
        self.logger.setLevel(logging.DEBUG)
        self.logger.info("create ClientendHandler")

    def __call__(self, msg):
        try:
            self.logger.info(
                "handle frontend msg %s " % msg)  # from client handle
            self.logger.info(
                "self.workerManager.available_workerList is %s" % str(
                    self.workerManager.available_workerList))
            self.logger.info("self.workerManager.client:qWorkerMap is %s" % str(
                self.workerManager.clientWorkerMap))
            client_addr, request = msg
            if client_addr in self.workerManager.clientWorkerMap:
                worker_id = self.workerManager.clientWorkerMap[client_addr]
            else:
                # TODO this is just a simple demo
                # TODO which worker should handle the client need more effort
                # TODO when there is no empty worker
                # TODO The old worker can still worker for new client?
                if len(self.workerManager.available_workerList) == 0:
                    worker_id = self.workerManager.workers[0]
                else:
                    worker_id = self.workerManager.available_workerList.pop()
                    self.workerManager.clientWorkerMap.update(
                        {client_addr: worker_id})

            # This was just send to the worker
            # worker_id was something identify
            self.worker_backend.send_multipart(
                [worker_id, client_addr, ''.encode(), request])

            self.logger.info("available_worker num %d :\n %s" % (
                len(self.workerManager.available_workerList),
                str(self.workerManager.available_workerList)))
            self.logger.info("client handle one finish")
        except:
            self.logger.error(traceback.format_exc())


class WorkersHandler(MessageHandler):
    """
    handler msg from worker and use frontend send msg to client
    """

    def __init__(self, worker_backend, client_frontend, workerManager, stop,
            logHandler):
        super(WorkersHandler, self).__init__()
        self.worker_backend = worker_backend
        self.client_frontend = client_frontend
        self._stop = stop
        self.workerManager = workerManager
        self.logHandler = logHandler
        self.logger = logging.getLogger("WorkerendHandler")
        self.logger.setLevel(logging.DEBUG)

    def __call__(self, msg):

        try:
            self.logger.info("workend handle one msg start : %s" % str(msg))
            self.logger.info(
                "self.workerManager.available_workerList is %s" % str(
                    self.workerManager.available_workerList))
            self.logger.info("self.workerManager.clientWorkerMap is %s" % str(
                self.workerManager.clientWorkerMap))

            print("---------------", msg)
            # when socket type id REQ
            # --------------- [b'Worker-1', b'', b'READY']
            # what is b ''
            # when socket type is declare
            # --------------- [b'Worker-1', b'READY']
            worker_addr, client_addr = msg[:2]

            server_status = None  #

            if len(msg) > 2:
                empty, reply = msg[2:]  # the send msg is  [address, '', 'OK' ]

            # assert self.workerManager.available_workers < WORKER_NUM
            if worker_addr not in self.workerManager.workers:
                self.workerManager.workers.append(worker_addr)

            old_availableworkers = len(self.workerManager.available_workerList)

            if client_addr == b"READY":
                if worker_addr not in self.workerManager.available_workerList:
                    self.workerManager.available_workerList.append(worker_addr)

            if client_addr != b"READY" and server_status != b"WorkerReset":
                # print("-------- reply")
                empty, reply = msg[2:]  # the send msg is  [address, '', 'OK' ]
                print("-------- reply -------", reply)
                assert empty == b""
                self.logger.info("Worker send client msg is %s " % str(msg))
                try:
                    self.client_frontend.send_multipart([client_addr, reply],
                        zmq.NOBLOCK)
                except:
                    self.logger.error(traceback.format_exc())

            if len(
                    self.workerManager.available_workerList) == 1 and old_availableworkers == 0:
                # on first recv, start accepting frontend messages
                self.logger.info("start client_frontend to recv client requst ")
                # 有一个 worker 可以用了
                self.client_frontend.on_recv(
                    ClientendHandler(self.worker_backend, self.client_frontend,
                        self.workerManager, self._stop, self.logHandler))

            self.logger.info("available_worker num %d :\n %s" % (
                len(self.workerManager.available_workerList),
                str(self.workerManager.available_workerList)))

        except:

            self.logger.error(traceback.format_exc())


class WorkProcess(multiprocessing.Process):
    """
    WorkerProcess , use tho handle every client, the total numbers should can be config
    """

    def __init__(self, workaddr, workid, logHandler, workdir):
        """
        every worker process should has it own logger
        """
        super(WorkProcess, self).__init__()
        self.workid = workid
        self.workaddr = workaddr
        self.workdir = workdir

        self.zproxyServerLogHandler = logHandler
        self.zproxyServerLoger = logging.getLogger(
            "Child-WorkProcess-%d" % self.workid)
        self.worker_log_handler = None
        self.zproxyWProcessLoger = logging.getLogger(
            "WorkProcess-%d" % self.workid)

    def signal_term_handler(self, sig, frame):
        # handle child process

        try:
            if self.zproxyWorker:
                self.zproxyWorker.poller.unregister(
                    self.socket)  # self.zproxyWorker.close()
            self.socket.close()
            self.context.term()
            self.worker_log_handler.close()
            sys.exit(0)
        except:
            os.kill(os.getpid(), signal.SIGKILL)

    def init_socket(self):

        signal.signal(signal.SIGTERM, self.signal_term_handler)
        self.zproxyServerLoger.info(
            "process id %d is begin runing " % os.getpid())
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.DEALER)
        #  will block on send unless it has successfully received a reply back.
        # self.socket.linger = 0
        self.linger = 20
        self.identity = "Worker-%d" % (self.workid)
        self.socket.setsockopt(zmq.IDENTITY, self.identity.encode())
        self.socket.setsockopt(zmq.LINGER, self.linger)
        self.socket.connect(self.workaddr)  # it is connecting

    def reset_socket(self):

        """
        Worker using REQ socket to do LRU routing
        """
        try:
            if self.zproxyWorker:
                self.zproxyWorker.poller.unregister(
                    self.socket)  # self.zproxyWorker.poller.close()
            self.zproxyServerLoger.error("reset 0")
            self.socket.disconnect(self.workaddr)
            self.socket.close()
            self.context.term()

        except:
            self.zproxyServerLoger.error(traceback.format_exc())

        # recreate it
        self.zproxyServerLoger.error("reset 1")
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.zproxyServerLoger.error("reset 2")
        self.identity = "Worker-%d" % (self.workid)
        self.socket.setsockopt(zmq.IDENTITY, self.identity.encode())
        self.socket.setsockopt(zmq.LINGER, self.linger)
        self.socket.connect(self.workaddr)
        self.zproxyServerLoger.error("reset 3")

    def run(self):

        self.init_socket()
        self.socket.send_string("READY")  # This is for worker manager
        reset = 0
        self.zproxyWorker = None
        resultPack = msgpack.packb({"ser_status": "None", "rspmsg": "None"})

        try:

            address, _, msg = self.socket.recv_multipart()
            print('------------- work recv msg ', msg)
            # buffer_len = 1024
            data = msg
            data_str = data.decode()
            assert data_str == 'init'  # client init
            self.socket.send_multipart([address, ''.encode(), b"*"],
                zmq.NOBLOCK)
            state = ProcessingState.WAIT_FOR_MSG

            while True:
                if reset == 1:
                    try:
                        self.reset_socket()
                        self.zproxyServerLoger.error("reset 4")
                        restmsg = msgpack.packb(
                            {"ser_status": "WorkerReset", "rspmsg": resultPack})

                        self.socket.send_multipart(
                            [address, ''.encode(), restmsg], zmq.NOBLOCK)
                        self.zproxyServerLoger.error("reset 5")
                        reset = 0
                    except:
                        self.zproxyServerLoger.error(traceback.format_exc())

                try:
                    # listeng on itself worker-sock
                    address, empty, data = self.socket.recv_multipart()
                    # buffer_len = 1024
                    data_str = data.decode()
                    print(f"recv {data}")
                    if not data:
                        break
                    for i, ch in enumerate(data_str):
                        if state == ProcessingState.WAIT_FOR_MSG:
                            # print("ch:", ch)
                            if ch == '^':
                                state = ProcessingState.IN_MSG
                        elif state == ProcessingState.IN_MSG:
                            if ch == '$':
                                state = ProcessingState.WAIT_FOR_MSG
                            else:
                                self.socket.send_multipart(
                                    [address, ''.encode(),
                                     chr(ord(data_str[i]) + 1).encode()],
                                    zmq.NOBLOCK)  # conn.send(chr(ord(data_str[i]) + 1).encode())
                except:
                    resultPack = msgpack.packb({
                        "ser_status": "Failed", "rspmsg": traceback.format_exc()
                    })
                    self.zproxyServerLoger.error(traceback.format_exc())

        except zmq.ZMQError as zerr:
            # context terminated so quit silently
            if zerr.strerror == 'Context was terminated':
                return
            else:
                raise zerr
        except:
            self.zproxyServerLoger.error(traceback.format_exc())
            return


class ZproxyServer(ZmqProcess):
    """
    after make it as daemon process 
    """

    def __init__(self, workdir, worker_num):
        super(ZproxyServer, self).__init__()
        self.workerMangaer = WorkerManager()
        self.worker_backend = None
        self.client_frontend = None
        self.worker_addr = "ipc:///tmp/workers"
        self.worker_num = worker_num
        self.client_addr = "tcp://*:9000"
        self.ipcFile = "/tmp/zproxy_clients"  # worker can be tcp too
        self.logHandler = None
        self.logger = logging.getLogger("zproxy_server")
        self.logger.setLevel(logging.INFO)
        self.workdir = workdir
        self.childList = []

    def signal_term_handler(self, sig, frame):
        # handle child process
        self.zproxyProcessList = self.childList
        self.logger.error("self %s , self.zproxyProcess %s" % (
            str(self), str(self.zproxyProcessList)))

        for oneProcess in self.zproxyProcessList:
            try:
                self.logger.info(
                    "killing chilid in signal pid is {}".format(oneProcess.pid))
                os.kill(oneProcess.pid, signal.SIGTERM)
                oneProcess.join()
            except Exception as e:
                err = sys.exc_info()[0]
                self.logger.error("{} {}".format(err, traceback.format_exc()))

        try:
            self.stop()
            super(ZproxyServer, self).context.term()
            # self.logHandler.close()
            sys.exit(-1)
        finally:
            os.kill(os.getpid(), signal.SIGKILL)

    def setup(self):
        """
        Sets up PyZMQ and creates all streams.
        """
        super(ZproxyServer, self).setup()
        self.worker_backend = self.stream(zmq.ROUTER, self.worker_addr,
            bind=True)
        self.client_frontend = self.stream(zmq.ROUTER, self.client_addr,
            bind=True)
        self.worker_backend.on_recv(
            WorkersHandler(self.worker_backend, self.client_frontend,
                self.workerMangaer, self.stop, self.logHandler))

    def run(self):
        """
        Sets up everything and starts the event loop.
        """
        signal.signal(signal.SIGTERM, self.signal_term_handler)
        for i in range(self.worker_num):
            aWorkProcess = WorkProcess(self.worker_addr, i, self.logHandler,
                self.workdir)
            aWorkProcess.daemon = True
            aWorkProcess.start()
            self.childList.append(aWorkProcess)

        time.sleep(1)
        self.setup()
        self.loop.start()

    def stop(self):
        self.loop.stop()


def options(args):
    argParser = argparse.ArgumentParser()
    argParser.add_argument("-n", dest="worknum", default="2",
        help="max work num")
    argParser.add_argument("-w", dest="workdir", default=".",
        help="ZproxyServer")
    zproxy_arg = argParser.parse_args(args)
    return zproxy_arg


if __name__ == '__main__':
    zproxy_arg = options(sys.argv[1:])
    workdir = os.path.abspath(".")
    worker_num = int(zproxy_arg.worknum)
    try:
        zproxy_server = ZproxyServer(workdir,
            worker_num)  # we can get server's child.pid
        zproxy_server.start()
        zproxy_server.join()
    except Exception as e:
        print(traceback.format_exc())