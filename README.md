## zmq implement for  [concurrent_server](https://github.com/jiamo/concurrent_server)
Zmq concept was complicated. especially for `zmq.DEALER` and `zmq.ROUTER`
The core idea is something like

![alt text](https://allstoalls.com/files/zmq_server.png)

## run

run server:  

    python -m zmq_server.zproxy_server

run client:  

    python -m zproxy.simple_client


