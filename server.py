import hashlib
import math
import time
import zmq
import json
import threading

PORT1 = '8082'
PORT2 = '8083'
PORT3 = '8084'
PORT4 = '8085'

class Node():
    def __init__(self, address, introduction_node = None, debug_print = False):
        self.address = address
        self.id = lambda input_to_id : 0
        self.debug_print = debug_print      
        self.context = zmq.Context()
        self.size = 64
        self.length_verify = 3        
        self.verify = [(self.id, self.address) for i in range(self.length_verify)]
        self.start = lambda i : (self.id + 2**(i)) % 2**self.size
        self.finger_table = [None for i in range(self.size)]
        self.waiting_time = 10
        self.predecessor = None
    
        #crear comandos???????????????????????????????????????????????????????????????????
        self.commands = {"join": self.command_join, "find_successor": self.command_find_successor,
                         "find_predecessor": self.command_find_predecessor}
        self.commands_request = {}

        if self.debug_print: print("Started node ", (self.id, self.addr))

        #Falta introducir nodo a la red??????????????????????????????????????????????????


    def waiting_for_commands(self, client_request):
        
        self.sock_rep = self.context.socket(zmq.REP)
        self.sock_rep.bind("tcp://" + self.addr)    
                
        while True:

            print("Waiting")
            
            buff = self.sock_rep.recv_json()

            if buff['command_name'] in self.commands:
                
                print(buff)
                if buff['command_name'] in self.commands_request:
                    self.commands[buff["command_name"]](**buff["method_params"], sock_req = client_request)
                else:
                    self.commands[buff["command_name"]](**buff["method_params"])



    def command_join(self):
        self.sock_rep.send_json({"response": "ACK_to_join", "return_info": {}})

    def command_find_successor(self):
        id_ip = (self.finger_table[0][0],self.finger_table[0][1])
        self.sock_rep.send_json({"response": "ACK", "return_info": id_ip})

    def command_find_predecessor(self):
        id, ip = self.predecessor
        self.sock_rep.send_json({"response": "ACK", "return_info": {"predecessor_id": id, "predecessor_ip": ip}, "procedence_address": self.address } )






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
        socket = self.socket_pull
        address = "tcp://"+ self.antecessor_ip +":"+ PORT4
        self.socket_pull.bind(address) 
        objects_lock = threading.Lock()
        #Open Thread
        get_objects_thread = threading.Thread(target = self.get_objects_thread_funct, args = (self,objects_lock,self.socket_pull))
        get_objects_thread.start()
        get_objects_thread.join()
        
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
        
