
import hashlib
import time
from requests import request
import zmq
import netifaces as ni
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
    def broadcast_thread_funct(self,lock,socket)-> None:
        while True:
            recv_json =  json.loads(socket.recv_json())
            if recv_json: #if there is something in recv_json
                if("I-get-in" in recv_json):
                    lock.acquire()
                    ret_json = {"send-me-confirmation":self.ip}
                    address = "tcp://"+ recv_json["I-get-in"] +":"+ PORT
                    self.socket_push.bind(address)
                    self.socket_push.send_json(ret_json)
                    lock.release()
                    
        
    def pull_thread_funct(self,lock,socket)-> None:
        while True:
            recv_json =  json.loads(socket.recv_json())
            if recv_json: #if there is something in recv_json
                if("my-confirmation" in recv_json):
                    lock.acquire()
                    ret_json = {}
                    ret_json["you-are-in"]["successor_id"] = self.id
                    ret_json["you-are-in"]["successor_ip"] = self.ip
                    ret_json["you-are-in"]["antecessor_id"] = self.antecessor_id
                    ret_json["you-are-in"]["antecessor_ip"] = self.antecessor_ip
                    address = "tcp://"+ recv_json["my-confirmation"] +":"+ PORT
                    self.socket_push.bind(address)
                    self.socket_push.send_json(ret_json)
                    lock.release()
                if("give-me-my-info" in recv_json):
                    lock.acquire()
                    chord_successor = {}
                    for object_id in self.chord:
                        if object_id >= (2**recv_json["give-me-my-info"]["id"]) and object_id < (2** self.id):
                            chord_successor[object_id] = self.chord[object_id]
                            del(self.chord[object_id])
                    finger_table = self.finger_table
                    ret_json = {"here-you-are": {"chord":chord_successor, "finger-table":finger_table}}
                    address = "tcp://"+ recv_json["give-me-my-info"]["ip"] +":"+ PORT
                    self.socket_push.bind(address)
                    self.socket_push.send_json(ret_json)
                    
                    #Update my finger table
                    new_element ={"id":recv_json["give-me-my-info"]["id"],"ip":recv_json["give-me-my-info"]["ip"]}
                    self.finger_table.pop()
                    self.finger_table.insert(0,new_element)
                    
                    #Update finger table of other nodes
                    ret_json = {"update-finger-table": {"new-element": new_element, "count": 6}}
                    address = "tcp://"+ self.successor_id +":"+ PORT
                    self.socket_push.bind(address)
                    self.socket_push.send_json(ret_json)
                    self.successor_id = recv_json["give-me-my-info"]["id"]
                    self.successor_ip = recv_json["give-me-my-info"]["ip"]
                    lock.release()
                if "update-finger-table" in recv_json:
                    lock.acquire()
                    new_element = recv_json["update-finger-table"]["new-element"]
                    self.finger_table.pop()
                    self.finger_table.insert(0,new_element)
                    count = recv_json["update-finger-table"]["new-element"]
                    if count > 1:
                        ret_json = {"update-finger-table": {"new-element": new_element, "count": count-1}}
                        address = "tcp://"+ self.successor_id +":"+ PORT
                        self.socket_push.bind(address)
                        self.socket_push.send_json(ret_json)
                    lock.remove()
                    
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
                if("send-me-confirmation" in recv_json and not recived):
                    lock.acquire()
                    ret_json = {"my-confirmation":self.ip}
                    address = "tcp://"+ recv_json["send-me-confirmation"] +":"+ PORT
                    self.socket_push.bind(address)
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
        address = "tcp://"+ self.antecessor_ip +":"+ PORT 
        socket.bind(address) 
        ret_json = {"give-me-my-info": { "ip":self.ip,"id":self.id}}
        socket.send_json(ret_json)
        
        # get message from antecessor
        #-----------------------------------------------------------------#
        ##----------ESTA BIEN TOMAR LOS DATOS DE ESA FORMA?--------------##
        socket = self.socket_pull
        address = "tcp://"+ self.antecessor_ip +":"+ PORT
        self.socket_pull.bind(address) 
        objects_lock = threading.Lock()
        #Open Thread
        get_objects_thread = threading.Thread(target = get_objects_thread_funct, args = (self,objects_lock,self.socket_pull))
        get_objects_thread.start()
        get_objects_thread.join()
        #-----------------------------------------------------------------#
        
        
        def get_objects_thread_funct(self, lock, socket):
            start_time = time.time()
            recived = False
            while True:
                actual_time = time.time() 
                if(actual_time-start_time > 30):
                    break
                recv_json =  json.loads(socket.recv_json())
                if ("here-you-are" in recv_json and not recived):
                    lock.acquire()
                    self.chord = recv_json["here-you-are"]["chord"]
                    self.finger_table = recv_json["here-you-are"]["finger-table"]
                    lock.release()
                    recived = True
                    break
            return 

    def calculate_id_in(self, ip_request, initial_id, best_id, best_ip_to_in, best_score):

        #La idea es calcular el mayor espaciamiento entre dos nodos en el chord
        # best_score inicia en 1 patra que no se elijan nodos consecutivos

        if initial_id == None:  # Si es none es porque este es el primer request
            initial_id = self.id
        
        if len(self.finger_table) == 0: # Si solo hay un nodo en el chord
            if self.id < n - self.id:
                best_id = n + self.id
            else:
                best_id = self.id

            if best_id % 2 == 0:
                best_id = best_id / 2
            else:
                best_id = (best_id / 2) + 1
            # Enviar mensaje al ip_request de que fue entrado al chord
            # Entrar al nodo

        # Primero calculamos el espaciamiento entre esta pc y la siguiente
        
        if self.id < self.successor_id:
            if self.successor_id - self.id > best_score:
                best_score = self.successor_id - self.id
                best_id = self.id + self.successor_id
                if best_id % 2 == 0:
                    best_id = best_id / 2
                else:
                    best_id = (best_id / 2) + 1
                best_ip_to_in = self.ip
        else:
            if n - self.id + self.successor_id > best_score:
                best_score = n - self.id + self.successor_id

                best_id = self.id + n + self.successor_id
                
                if best_id % 2 == 0:
                    best_id = best_id / 2
                else:
                    best_id = (best_id / 2) + 1

                if best_id > n:
                    best_id = best_id - n
                best_ip_to_in = self.ip

        # Luego revisamos todos los espaciamientos en la finger table

        for i in range(len(self.finger_table) - 1):

            first_id = self.finger_table[i][0]
            second_id = self.finger_table[i + 1][0]

            if self.finger_table[i][0] == initial_id:
                # Ya con esto recorrimos todo el chord y encontramos el mejor espaciamiento
                # Ahora le enviamos ese mensaje al nodo designado para que reciba al nuevo nodo
                return


            if  first_id < second_id:
                if first_id - second_id > best_score:
                    best_score = first_id - second_id
                    best_id = first_id + second_id
                    if best_id % 2 == 0:
                        best_id = best_id / 2
                    else:
                        best_id = (best_id / 2) + 1
                    best_ip_to_in = self.ip
            else:
                if n - first_id + second_id > best_score:
                    best_score = n - first_id + second_id

                    best_id = first_id + n + second_id
                    
                    if best_id % 2 == 0:
                        best_id = best_id / 2
                    else:
                        best_id = (best_id / 2) + 1

                    if best_id > n:
                        best_id = best_id - n
                    best_ip_to_in = self.ip

        # Enviar todos los datos actuales al nodo en la última posición de la finger table