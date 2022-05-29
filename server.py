
import hashlib
import zmq
import netifaces as ni
import pickle
import json

PORT = '8002'

class Node():
    def __init__(self):
        self.ip, self.broadcast = self.get_ip_broadcast()
        self.id = self.get_id(self.ip)
        self.chor = [] # {hash: obj_addr}
        self.successor_id = None
        self.successor_ip = None
        self.antecessor_id = None
        self.antecessor_ip = None
        self.context = zmq.Context(io_threads= 1)
        self.socket_pub = self.context.socket(zmq.PUB)
        self.socket_sub = self.context.socket(zmq.SUB)
        address = "tcp://"+ self.broadcast +":"+ PORT
        #-----------------------------------------------------------------#
        ##----COMO PUEDO TOMAR LOS DATOS CUANDO ME HACEN UN BROADCAST?----##
        self.socket_sub.bind(address) 
        address = "tcp://"+ self.ip +":"+ PORT
        ##----COMO PUEDO TOMAR LOS DATOS piden unirse o la tabla chor?----##
        self.socket_pull.bind(address) 
        #-----------------------------------------------------------------# 
        self.socket_push = self.context.socket(zmq.PUSH)
        self.socket_pull = self.context.socket(zmq.PULL)
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
        socket.send_string('I-get-in-bitches')
        # get message from successor
        #-----------------------------------------------------------------#
        ##----ESTA BIEN TOMAR LOS DATOS DE ESA FORMA?----##
        socket = self.socket_pull
        address = "tcp://"+ self.ip +":"+ PORT
        socket.bind(address)  
        recv_json =  json.loads(socket.recv_json())
        self.successor_id = recv_json["successor_id"]
        self.successor_ip = recv_json["successor_ip"]
        self.antecessor_id = recv_json["antecessor_id"]
        self.antecessor_ip = recv_json["antecessor_ip"]
        #-----------------------------------------------------------------#
        return
        
    def update_finger_table(self) -> None:
        # update my objects
        socket = self.socket_push 
        address = "tcp://"+ self.successor_ip +":"+ PORT 
        socket.bind(address) 
        socket.send("give-me-my-info")
        # get message from successor
        #-----------------------------------------------------------------#
        ##----ESTA BIEN TOMAR LOS DATOS DE ESA FORMA?----##
        socket = self.socket_pull
        address = "tcp://"+ self.ip +":"+ PORT
        socket.bind(address)  
        recv_json =  json.loads(socket.recv_json())
        self.chor = recv_json
        #-----------------------------------------------------------------#
        # update other objects
        address = "tcp://"+ self.antecessor_ip +":"+ PORT 
        socket.bind(address) 
        socket.send_json("{new-finger-table:"+self.finger_table+"}") 
        