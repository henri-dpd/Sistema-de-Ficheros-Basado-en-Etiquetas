import json
import threading
from wsgiref import validate
import zmq, time

HOST = '0.0.0.0'
PORT1 = '8000'
PORT2 = '8001'

class AccessServer:
    def __init__(self, name, age):
        self.ip_table = {} # {ip: last_update}
        self.context = zmq.Context()
        self.socket_pull = self.context.socket(zmq.PULL) # create a pull socket 
        self.socket_push = self.context.socket(zmq.PUSH) # create a push socket 
        address = "tcp://"+ HOST +":"+ PORT1 # how and where to communicate
        self.socket_pull.bind(address) # bind socket to the address
        pull_lock = threading.Lock()
        #Open Thread
        pull_thread = threading.Thread(target = self.pull_thread_funct, args = (self, pull_lock,self.socket_sub))
        pull_thread.start()
        pull_thread.join()
        #Validation of ip table
        validate_lock = threading.Lock()
        #Open Threas
        validate_thread = threading.Thread( target= self.validate_ip_table, args= (self, validate_lock))
        validate_thread.start()
        validate_thread.join()
    
    def pull_thread_funct(self, lock, socket):
        while True:
            recv_json =  json.loads(socket.recv_json())
            if recv_json: #if there is something in recv_json
                if "get-ip-table" in recv_json:
                    lock.aquire()
                    ret_json = {"ip-table": list(self.ip_table.keys)}
                    address = "tcp://"+ recv_json["get-ip-table"] +":"+ PORT2
                    self.socket_push.bind(address)
                    self.socket_push.send_json(ret_json) # publish the current time
                    lock.release()
                if "add-my-ip" in recv_json:
                    lock.aquire()
                    self.ip_table[recv_json["get-ip-table"]] = 0
                    lock.release()
                if "I-am-still-in" in recv_json:
                    lock.aquire()
                    self.ip_table[recv_json["i-am-still-in"]] = 0
                    lock.release()
                    
    
    def validate_ip_table(self, lock):
        while True:
            time.sleep(10)
            lock.aquire()
            for ip in self.ip_table:
                if self.ip_table[ip] > 3:
                    del(self.ip_table[ip])
                else:
                    self.ip_table[ip] += 1
                    ret_json = {"are-you-still-in": HOST}
                    address = "tcp://"+ ip +":"+ PORT2
                    self.socket_push.bind(address)
                    self.socket_push.send_json(ret_json) # publish the current time
            lock.release()