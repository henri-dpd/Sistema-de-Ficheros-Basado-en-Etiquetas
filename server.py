
import hashlib
import zmq
import netifaces as ni
import pickle

HOST = '0.0.0.0'
PORT = '8002'

class Node():
    def __init__(self):
        self.ip = self.get_ip()
        self.id = self.get_id(self.ip)
        self.successor_id = None
        self.successor_ip = None
        self.antecessor_id = None
        self.antecessor_ip = None
        self.context = zmq.Context(io_threads= 1)
        self.get_in()
        self.finger_table = [] # [("id1", "ip1"), ("id2", "ip2")]
        self.my_objects = {} # {"id1": "tal lugar", "id2": "mas cual lugar"}
        self.update_finger_table()
        """ 
        self.socket_to_successor = self.context.socket(zmq.PUSH)
        self.socket_from_antecessor = self.context.socket(zmq.PULL)
        self.socket_from_antecessor.bind('tcp://' + self.antecessor_ip + ':5555')
        """

    # get ip of the pc
    def get_ip(self) -> str:
        interfaces = ni.interfaces()
        if 'vmnet1' in interfaces: 
            return ni.ifaddresses('vmnet1')[ni.AF_LINK][0]['addr']
        elif 'vmnet8' in interfaces: 
            return ni.ifaddresses('vmnet8')[ni.AF_LINK][0]['addr']
        elif 'enp3s0f1' in interfaces: 
            return ni.ifaddresses('enp3s0f1')[ni.AF_LINK][0]['addr']
        else:
            return ni.ifaddresses(interfaces[0])[ni.AF_LINK][0]['addr']

    # calculate id using sha hash
    def get_id(self, ip:int)-> str:
        sha = hashlib.sha1()
        sha.update(ip.encode('ascii'))
        return  int(sha.hexdigest() ,16)

    # send broadcast message to get in
    def get_in(self) -> None:
        socket = self.context.socket(zmq.PUB)
        address = "tcp://"+ HOST +":"+ PORT
        socket.bind(address)  
        socket.send_string('I-get-in-bitches')
        return
        
    def update_finger_table(self) -> None:
        # update my objects
        socket = self.context.socket(zmq.PUSH) 
        address = "tcp://"+ HOST +":"+ PORT 
        socket.bind(address) 
        socket.send("give-me-my-info")
        # update other objects
        socket2 = self.context.socket(zmq.PUSH) 
        address = "tcp://"+ HOST +":"+ PORT 
        socket2.bind(address) 
        socket2.send_json("{new-finger-table:"+self.finger_table+"}") 
    
    