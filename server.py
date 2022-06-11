
import hashlib
import math
import time
import zmq
import netifaces as ni
import json
import threading

PORT1 = '8001'
PORT2 = '8002'
PORT3 = '8003'
PORT4 = '8004'

class Node():
    def __init__(self, number_nodes = 64):
        self.number_nodes = number_nodes
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
        self.socket_push = self.context.socket(zmq.PUSH)
        self.socket_pull = self.context.socket(zmq.PULL)
        #------------------------------------------------------------------#
        ##----COMO PUEDO TOMAR LOS DATOS CUANDO ME HACEN UN BROADCAST?----##
        ##----EL ADDRESS ES DEL IP DE BROADCAST O DEL IP DEL NODO?--------##
        address = "tcp://"+ self.broadcast +":"+ PORT2
        self.socket_sub.bind(address) 
        broadcast_lock = threading.Lock()
        #Open Thread
        broadcast_thread = threading.Thread(target = self.broadcast_thread_funct, args = (self,broadcast_lock,self.socket_sub))
        broadcast_thread.start()
        broadcast_thread.join()
        ##----COMO PUEDO TOMAR LOS DATOS piden unirse o la tabla chord?----##
        address = "tcp://"+ self.ip +":"+ PORT4
        self.socket_pull.bind(address) 
        pull_lock = threading.Lock()
        #open thread
        pull_thread = threading.Thread( target = self.pull_thread_funct, args = (self, pull_lock, self.socket_pull))
        pull_thread.start()
        pull_thread.join()
        self.get_in()
        self.finger_table = [] # [("id1", "ip1"), ("id2", "ip2")]
        self.my_objects = {} # {"id1": "tal lugar", "id2": "mas cual lugar"}
        self.update_finger_table()

        # Inicialización de todo aquello relacionado con etiquetas:
        self.label_finger_table = {} # { "Fotos de perros" : [id_1, id_2, id_3...], "Recetas de Comida" : [id_4, id_5...]}
        self.labels = {} # {"Fotos de Perros" : counter_1, "Recetas de Comida": counter_2, ....}

    # get ip of the pc
    def get_ip_broadcast(self) -> str:
        return '127.10.0.1', '127.10.255.255'
        interfaces = ni.interfaces()
        if 'vmnet1' in interfaces: 
            return ni.ifaddresses('vmnet1')[ni.AF_INET][0]['addr'], ni.ifaddresses('vmnet1')[ni.AF_INET][0]['broadcast']
        elif 'vmnet8' in interfaces: 
            return ni.ifaddresses('vmnet8')[ni.AF_INET][0]['addr'], ni.ifaddresses('vmnet8')[ni.AF_INET][0]['broadcast']
        elif 'docker0' in interfaces:
            return ni.ifaddresses('docker0')[ni.AF_INET][0]['addr'], ni.ifaddresses('docker0')[ni.AF_INET][0]['broadcast']
        elif 'enp3s0f1' in interfaces:
            return ni.ifaddresses('enp3s0f1')[ni.AF_INET][0]['addr'], ni.ifaddresses('enp3s0f1')[ni.AF_INET][0]['broadcast']
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
        address = "tcp://"+ self.broadcast +":"+ PORT1
        socket.bind(address)  
        socket.send_string('{"I-get-in": "'+ self.ip +'"}')
        # get message from successor
        #-----------------------------------------------------------------#
        ##----------ESTA BIEN TOMAR LOS DATOS DE ESA FORMA?--------------##
        socket = self.socket_pull
        address = "tcp://"+ self.ip +":"+ PORT4
        # socket.bind(address)
        lock = threading.Lock()
        #Open Thread
        t = threading.Thread(target = self.request, args = (self,lock,socket))
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
                    address = "tcp://"+ recv_json["I-get-in"] +":"+ PORT3
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
                    address = "tcp://"+ recv_json["my-confirmation"] +":"+ PORT3
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
                    address = "tcp://"+ recv_json["give-me-my-info"]["ip"] +":"+ PORT3
                    self.socket_push.bind(address)
                    self.socket_push.send_json(ret_json)
                    
                    #Update my finger table
                    new_element ={"id":recv_json["give-me-my-info"]["id"],"ip":recv_json["give-me-my-info"]["ip"]}
                    self.finger_table.pop()
                    self.finger_table.insert(0,new_element)
                    
                    #Update finger table of other nodes
                    if len(self.finger_table) < math.log2(self.number_nodes)-1: 
                        for i in self.finger_table:
                            ret_json = {"update-finger-table": {"new-element": new_element, "count": 1}}
                            address = "tcp://"+ i["ip"] +":"+ PORT3
                            self.socket_push.bind(address)
                            self.socket_push.send_json(ret_json)
                    else:
                        ret_json = {"update-finger-table": {"new-element": new_element, "count": math.log2(self.number_nodes)-1}}
                        address = "tcp://"+ self.successor_ip +":"+ PORT3
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
                        address = "tcp://"+ self.successor_ip +":"+ PORT3
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
                    address = "tcp://"+ recv_json["send-me-confirmation"] +":"+ PORT3
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
        if self.antecessor_ip == None:
            self.successor_id =  self.id
            self.successor_ip =  self.ip
            self.antecessor_id = self.id 
            self.antecessor_ip = self.ip 
            return
        # update my objects
        socket = self.socket_push
        address = "tcp://"+ self.antecessor_ip +":"+ PORT3
        socket.bind(address) 
        ret_json = {"give-me-my-info": { "ip":self.ip,"id":self.id}}
        socket.send_json(ret_json)
        
        # get message from antecessor
        #-----------------------------------------------------------------#
        ##----------ESTA BIEN TOMAR LOS DATOS DE ESA FORMA?--------------##
        socket = self.socket_pull
        address = "tcp://"+ self.antecessor_ip +":"+ PORT4
        self.socket_pull.bind(address) 
        objects_lock = threading.Lock()
        #Open Thread
        get_objects_thread = threading.Thread(target = self.get_objects_thread_funct, args = (self,objects_lock,self.socket_pull))
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
        
    # Método para buscar en qué pc se encuentra una etiqueta y pedirle que la devuelva
    def search_by_label(self, label, ip_request, initial_request):

        self.return_by_label(label, ip_request)

        # Aquí guardaremos las pc que conocemos que tienen dicha etiqueta
        labels_id = []

        if label in self.label_finger_table:
            labels_id = self.label_finger_table[label]
        
        for (id, ip) in self.finger_table: # Viajamos por toda la finger table

            if ip == initial_request: #Si ya le dimos toda la vuelta al chrod nos detenemos
                return
            
            if (id, ip) == labels_id[len(labels_id) - 1]:
                request_json = {"label" : label, "ip_request":ip_request, initial_request:"initial_request"}
                address = "tcp://"+ ip +":"+ PORT3
                self.socket_push.bind(address)
                self.socket_push.send_json(request_json)
                return

            if id in labels_id:
                request_json = {"label" : label, "ip_request":ip_request, initial_request:None}
                address = "tcp://"+ ip +":"+ PORT3
                self.socket_push.bind(address)
                self.socket_push.send_json(request_json)
                

    def return_by_label(self, label, ip_request):
        if label in self.labels:
            ################
            # Enviar todos los archivos correspondientes al ip_request
            pass


    # Método para calcular el id que debe tener en el chord una pc que desea entrar
    def calculate_id_in(self, ip_request, initial_id, best_id, best_ip_to_in, best_score):

        #La idea es calcular el mayor espaciamiento entre dos nodos en el chord
        # best_score inicia en 1 para que no se elijan nodos consecutivos

        if initial_id == None:  # Si es none es porque este es el primer request
            initial_id = self.id
        
        if len(self.finger_table) == 0: # Si solo hay un nodo en el chord
            if self.id < self.number_nodes - self.id:
                best_id = self.number_nodes + self.id
            else:
                best_id = self.id

            if best_id % 2 == 0:
                best_id = best_id / 2
            else:
                best_id = (best_id / 2) + 1
            # Entrar al nodo
            # Enviar mensaje al ip_request de que fue entrado al chord
            

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
            if self.number_nodes - self.id + self.successor_id > best_score:
                best_score = self.number_nodes - self.id + self.successor_id

                best_id = self.id + self.number_nodes + self.successor_id
                
                if best_id % 2 == 0:
                    best_id = best_id / 2
                else:
                    best_id = (best_id / 2) + 1

                if best_id > self.number_nodes:
                    best_id = best_id - self.number_nodes
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
                if self.number_nodes - first_id + second_id > best_score:
                    best_score = self.number_nodes - first_id + second_id

                    best_id = first_id + self.number_nodes + second_id
                    
                    if best_id % 2 == 0:
                        best_id = best_id / 2
                    else:
                        best_id = (best_id / 2) + 1

                    if best_id > self.number_nodes:
                        best_id = best_id - self.number_nodes
                    best_ip_to_in = self.ip

        # Enviar todos los datos actuales al nodo en la última posición de la finger table