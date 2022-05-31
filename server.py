
import hashlib
import time
from requests import request
import zmq
import netifaces as ni
import pickle
import json
import threading

PORT = '8002'

class Node():
    def __init__(self):
        self.ip, self.broadcast = self.get_ip_broadcast()
        self.id = self.get_id(self.ip)
        self.chord = [] # {hash: obj_addr}
        self.successor_id = None
        self.successor_ip = None
        self.antecessor_id = None
        self.antecessor_ip = None
        self.context = zmq.Context(io_threads= 1)
        self.socket_pub = self.context.socket(zmq.PUB)
        self.socket_sub = self.context.socket(zmq.SUB)
        #------------------------------------------------------------------#
        ##----COMO PUEDO TOMAR LOS DATOS CUANDO ME HACEN UN BROADCAST?----##
        ##----EL ADDRESS ES DEL IP DE BROADCAST O DEL IP DEL NODO?--------##
        address = "tcp://"+ self.broadcast +":"+ PORT
        self.socket_sub.bind(address) 
        broadcast_lock = threading.Lock()
        #Open Thread
        broadcast_thread = threading.Thread(target = broadcast_thread_funct, args = (self,broadcast_lock,self.socket_pub))
        broadcast_thread.start()
        broadcast_thread.join()
        ##----COMO PUEDO TOMAR LOS DATOS piden unirse o la tabla chord?----##
        address = "tcp://"+ self.ip +":"+ PORT
        self.socket_pull.bind(address) 
        #------------------------------------------------------------------# 
        self.socket_push = self.context.socket(zmq.PUSH)
        self.socket_pull = self.context.socket(zmq.PULL)
        self.socket_pull.bind(self.ip)
        pull_lock = threading.Lock()
        #open thread
        pull_thread = threading.Thread( target= pull_thread_funct, args = (self, pull_lock, self.socket_pull))
        self.get_in()
        self.finger_table = [] # [("id1", "ip1"), ("id2", "ip2")]
        self.my_objects = {} # {"id1": "tal lugar", "id2": "mas cual lugar"}
        self.update_finger_table()

    # get ip of the pc
    def get_ip_broadcast(self) -> str:
        interfaces = ni.interfaces()
        if 'vmnet1' in interfaces: 
            return ni.ifaddresses('vmnet1')[ni.AF_INET][0]['addr'], ni.ifaddresses('vmnet1')[ni.AF_INET][0]['broadcast']
        elif 'vmnet8' in interfaces: 
            return ni.ifaddresses('vmnet8')[ni.AF_INET][0]['addr'], ni.ifaddresses('vmnet8')[ni.AF_INET][0]['broadcast']
        elif 'wlp2s0' in interfaces: 
            return ni.ifaddresses('wlp2s0')[ni.AF_INET][0]['addr'], ni.ifaddresses('wlp2s0')[ni.AF_INET][0]['broadcast']
        else:
            return ni.ifaddresses(interfaces[0])[ni.AF_INET][0]['addr'], ni.ifaddresses(interfaces[0])[ni.AF_INET][0]['broadcast']

    # calculate id using sha hash
    def get_id(self, ip:int)-> str:
        sha = hashlib.sha1()
        sha.update(ip.encode('ascii'))
        return  int(sha.hexdigest() ,16)

    def get_in(self) -> None:
        # send broadcast message to get in
        socket = self.socket_pub
        address = "tcp://"+ self.broadcast +":"+ PORT
        socket.bind(address)  
        socket.send_string('{"I-get-in": "'+ self.ip +'"}')
        # get message from successor
        #-----------------------------------------------------------------#
        ##----------ESTA BIEN TOMAR LOS DATOS DE ESA FORMA?--------------##
        socket = self.socket_pull
        address = "tcp://"+ self.ip +":"+ PORT
        socket.bind(address)
        lock = threading.Lock()
        #Open Thread
        t = threading.Thread(target = request, args = (self,lock,socket))
        t.start()
        t.join()
        
        #-----------------------------------------------------------------#
        return

    #subscriter to broadcast
    def broadcast_thread_funct(self,lock,socket_pub)->None:
        while True:
            recv_json =  json.loads(socket_pub.recv_json())
            if recv_json: #if there is something in recv_json
                if("I-get-in" in recv_json):
                    lock.acquire()
                    ret_json = {"send-me-confirmation":self.ip}
                    self.socket_push.bind(recv_json["I-get-in"])
                    self.socket_push.send_json(ret_json)
                    lock.release()
        
    def pull_thread_funct(self,lock,socket_pull)->None:
        while True:
            recv_json =  json.loads(socket_pull.recv_json())
            if recv_json: #if there is something in recv_json
                if("my-confirmation" in recv_json):
                    lock.acquire()
                    ret_json = {}
                    ret_json["you-are-in"]["successor_id"] = self.id
                    ret_json["you-are-in"]["successor_ip"] = self.ip
                    ret_json["you-are-in"]["antecessor_id"] = self.antecessor_id
                    ret_json["you-are-in"]["antecessor_ip"] = self.antecessor_ip
                    self.socket_push.bind(recv_json["my-confirmation"])
                    self.socket_push.send_json(ret_json)
                    lock.release()
    # wait request
    def request (self,lock,socket)-> json:
        start_time = time.time()
        recived = False
        while True:
            actual_time = time.time() 
            if(actual_time-start_time > 30):
                break
            recv_json =  json.loads(socket.recv_json())
            if recv_json: #if there is something in recv_json
                if("send-me-confirmation" and not recived):
                    lock.acquire()
                    ret_json = {"my-confirmation":self.ip}
                    self.socket_push.bind(recv_json["send-me-confirmation"])
                    self.socket_push.send_json(ret_json)
                    lock.release()
                    recived = True
                if("you-are-in" in recv_json):
                    lock.acquire() 
                    self.successor_id = recv_json["successor_id"]
                    self.successor_ip = recv_json["successor_ip"]
                    self.antecessor_id = recv_json["antecessor_id"]
                    self.antecessor_ip = recv_json["antecessor_ip"]
                    lock.release()
                    break # break out of loop and end
        return recv_json
        
    def update_finger_table(self) -> None:
        # update my objects
        socket = self.socket_push 
        address = "tcp://"+ self.successor_ip +":"+ PORT 
        socket.bind(address) 
        socket.send("give-me-my-info")
        # get message from successor
        #-----------------------------------------------------------------#
        ##----------ESTA BIEN TOMAR LOS DATOS DE ESA FORMA?--------------##
        socket = self.socket_pull
        address = "tcp://"+ self.ip +":"+ PORT
        socket.bind(address)  
        recv_json =  json.loads(socket.recv_json())
        self.chord = recv_json
        #-----------------------------------------------------------------#
        # update other objects
        socket = self.socket_push 
        address = "tcp://"+ self.antecessor_ip +":"+ PORT 
        socket.bind(address) 
        socket.send_json("{new-finger-table:"+self.finger_table+"}") 
        