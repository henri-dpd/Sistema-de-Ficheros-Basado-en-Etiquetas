
from functools import reduce
import hashlib
import os
from random import Random
import math
import re
from time import sleep
import time
from request import request
import zmq
import threading
from request import request

PORT1 = '8082'
PORT2 = '8083'
PORT3 = '8084'
PORT4 = '8085'

class Node():
    def __init__(self, address, introduction_node = None):
        self.address = address
        self.id = self.get_node_hash(address)
        self.context = zmq.Context()
        self.size = 64
        self.k = 3
        self.k_successor = [(self.id, self.address) for _ in range(self.k)]
        self.finger_table_length = int(math.log2(self.size))
        self.finger_table = [None for _ in range(self.size)]
        self.waiting_time = 10
        self.is_leader = False
        
        self.commands = {"join": self.command_join, 
                         "verify_alive_node": self.verify_alive_nodes,
                         "nodes_losses_fix_chord": self.nodes_losses_fix_chord,
                         "are_you_alive": self.command_are_you_alive,
                         "get_params": self.command_get_params, 
                         "get_prop": self.command_get_prop,
                         "get_predecessor": self.command_get_predecessor,
                         "update_predecessor": self.command_update_predecessor,
                         "get_successor": self. command_get_successor,
                         "find_successor": self.command_find_successor,
                         "find_predecessor": self.command_find_predecessor,
                         "calculate_id_in" : self.calculate_id_in,
                         "get_in_new_node" : self.get_in_new_node,
                         "replace_predeccessor" : self.replace_predeccessor,
                         "replace_finger_table_consecutive" : self.replace_finger_table_consecutive,
                         "get_in_to_chord_succefully" : self.get_in_to_chord_succefully,
                         "recv_file" : self.command_recv_file,
                         "send_file" : self.command_send_file,
                         "closest_predecessor_fing" : self.command_closest_predecessor_fing,
                         "get_k_list" : self.command_get_k_list,
                         "rect" : self.command_rect,
                         "stabilize" : self.command_stabilize
                         }

        self.commands_request = {"verify_alive_node", "it_lost_a_node", "calculate_id_in", "get_in_new_node", 
                                 "replace_finger_table_consecutive", "nodes_losses_fix_chord","rect", "stabilize",
                                 "find_successor", "find_predecessor"}

        print("Started node ", (self.id, self.address))
        client_requester = request(context = self.context)
        if introduction_node:
            introduction_node_id = self.get_node_hash(introduction_node)
            recieved_json = client_requester.make_request(json_to_send = {"command_name" : "join",
                                                                         "method_params" : {}, 
                                                                         "procedence_addr" : self.address}, 
                                                          destination_address = introduction_node,
                                                          destination_id = introduction_node_id)
            
            while recieved_json is client_requester.error:                
                client_requester.action_for_error(introduction_node)
                print("Enter address to retry ")
                introduction_node = input()
                print("Connecting now to ", (introduction_node))
                introduction_node_id = self.get_node_hash(introduction_node)
            recieved_json = client_requester.make_request(json_to_send = {"command_name" : "join",
                                                                         "method_params" : {}, 
                                                                         "procedence_addr" : self.address}, 
                                                          destination_address = introduction_node,
                                                          destination_id = introduction_node_id)
            while not self.execute_join(introduction_node, introduction_node_id, self.start(0), client_requester):
                client_requester.action_for_error(introduction_node)
                print("Enter address to retry ")
                introduction_node = input()
                introduction_node_id = self.get_node_hash(introduction_node)
                print("Connecting now to ", (introduction_node_id, introduction_node))                
            recieved_json = client_requester.make_request(json_to_send = {"command_name" : "join",
                                                                         "method_params" : {}, 
                                                                         "procedence_addr" : self.address}, 
                                                          destination_address = introduction_node,
                                                          destination_id = introduction_node_id)
        else:
            self.predecessor_address, self.predecessor_id = self.address, self.id
            self.is_leader = True
        self.execute(client_requester)
        
    def get_node_hash(self, address):
        summ = ''
        for x in address.split(":")[0].split(".") + [address.split(":")[1]]:
            summ += x
        return int(hashlib.sha1(bytes(summ, 'utf-8') ).hexdigest(),16)

    def start(self, i):
        return (self.id + 2**(i)) % 2**self.size

    def execute_join(self, introduction_node, introduction_node_id, id_to_found_pred, sock_req):
        recv_json = sock_req.make_request(json_to_send = {"command_name" : "find_predecessor",
                                                          "method_params" : {"id" : id_to_found_pred},
                                                          "procedence_address" : self.address},
                                          requester_object = self,
                                          method_for_wrap = "find_predecessor",
                                          destination_id = introduction_node_id,
                                          destination_address = introduction_node)
        if recv_json is sock_req.error_json:
            return False
        
        self.predeccesor_id, self.predeccesor_address = recv_json['return_info']['predecessor_id'], recv_json['return_info']['predecessor_address']
        recv_json = sock_req.make_request(json_to_send = {"command_name" : "get_k_list", 
                                                          "method_params" : {}, 
                                                          "procedence_address" : self.address}, 
                                          requester_object = self, 
                                          asked_properties = ("k_list",), 
                                          destination_id = recv_json['return_info']['predecessor_id'], 
                                          destination_address = recv_json['return_info']['predecessor_address'] )         
        if recv_json is sock_req.error_json:
            return False
        self.k_list = recv_json['return_info']['k_list']
        return True

    def command_find_predecessor(self, sock_req):
        print("entro comando find predcessor")
        predecessor_id, predecessor_address = self.find_predecesor(id, sock_req)

        self.sock_rep.send_json({"response": "ACK", "return_info": {"predecessor_id": predecessor_id, "predecessor_address": predecessor_address}, "procedence_addr": self.address } )
        
        
    def find_predecessor(self, id, sock_req):
        current_id = self.id
        current_k_list_id = self.k_list[0][0]
        self.finger_table[0] = self.k_list[0]
        current_address = self.address  
        while not self.between(id, interval = (current_id, current_k_list_id)) and current_k_list_id != id :
            recv_json_closest_predecessor = sock_req.make_request(json_to_send = {"command_name" : "closest_predecessor_fing",
                                                                                  "method_params" : {"id": id}, 
                                                                                  "procedence_addr" : self.address, 
                                                                                  "procedence_method": "find_predecesor"}, 
                                                                  method_for_wrap = 'closest_predecessor_fing', 
                                                                  requester_object = self, 
                                                                  destination_id = current_id, 
                                                                  destination_address = current_address)
            if recv_json_closest_predecessor is sock_req.error_json : 
                return None
            recv_json_k_list = sock_req.make_request(json_to_send = {"command_name" : "get_k_list", 
                                                                   "method_params" : {}, 
                                                                   "procedence_addr" : self.address, 
                                                                   "procedence_method" : "find_predecesor" }, 
                                                   requester_object = self, 
                                                   asked_properties = ("k_list", ), 
                                                   destination_id = recv_json_closest_predecessor['return_info'][0], 
                                                   destination_address = recv_json_closest_predecessor['return_info'][1] )
            if recv_json_k_list is sock_req.error_json:
                return None
            current_id, current_address = recv_json_closest_predecessor['return_info'][0], recv_json_closest_predecessor['return_info'][1]
            current_k_list_id = recv_json_k_list['return_info']['k_list'][0][0]               
        
        return current_id, current_address

    def command_closest_predecessor_fing (self, id, sock_req):        
        closest_id, closest_address = self.closest_pred_fing(id, sock_req)
        self.sock_rep.send_json({"response" : "ACK", "return_info" : (closest_id, closest_address), "procedence": self.address})


    def closest_predecessor_fing(self, id, sock_req):
        for i in range(self.size-1, -1, -1):            
            if self.finger_table[i] is None : continue 
            if self.between(self.finger_table[i][0], (self.id, id) ) :
                return self.finger_table[i]                
        return (self.id, self.address)

    def between(self, id, interval):
        if interval[0] < interval[1]:
            return id > interval[0] and id < interval[1] 
        return id > interval[0] or id < interval[1]

    def command_get_k_list(self):
        self.sock_rep.send_json( {"response": "ACK", "return_info": {"k_list" : self.k_list} } )
        
    def execute(self, client_requester):
        print("entro execute")
        thread_verify = threading.Thread(target = self.thread_verify, args =() )
        thread_verify.start()        
        self.waiting_for_commands(client_requester)
        
    def thread_verify(self):
        countdown = time()
        rand = Random()
        rand.seed()
        requester = request(context = self.context)
        choices = [i for i in range(self.size)]
        while True:
            if abs (countdown - time( ) ) > self.waiting_time:
                if self.predecessor_id != self.id:
                    self.stabilize(sock_req = requester)
                    if requester.make_request(json_to_send = {"command_name" : "rect", 
                                                              "method_params" : { "predecessor_id": self.id, 
                                                                                 "predecessor_address" : self.address }, 
                                                              "procedence_address" : self.address, 
                                                              "procedence_method": "thread_verify", 
                                                              "time": time()}, 
                                              destination_id = self.k_list[0][0], 
                                              destination_addr = self.k_list[0][1]) is requester.error_json:
                        requester.action_for_error(self.k_list[0][1])

                    index = rand.choice( choices )                    
                    self.finger_table[ index ] = self.find_successor(self.start(index), sock_req = requester)                    
                countdown = time()

    def command_stabilize(self, sock_req : request):
                                       
        recv_json_predecessor = sock_req.make_request(json_to_send = {"command_name" : "get_predecessor", 
                                                               "method_params" : {}, 
                                                               "procedence_address" : self.address, 
                                                               "procedence_method": "stabilize"}, 
                                               requester_object = self, 
                                               asked_properties = ('predecessor_id', 'predecessor_address'), 
                                               destination_id = self.k_list[0][0], 
                                               destination_address = self.k_list[0][1])
        if recv_json_predecessor is sock_req.error_json:
            sock_req.action_for_error(self.succ_list[0][1])
            self.k_list.pop(0)
            self.k_list += [(self.id, self.address)]
            return

        recv_json_k_list = sock_req.make_request(json_to_send = {'command_name' : "get_k_list", 
                                                                 'method_params' : {}, 
                                                                 'procedence_address' : self.address, 
                                                                 "procedence_method": "stabilize"}, 
                                                 requester_object = self, 
                                                 asked_properties = ("k_list",), 
                                                 destination_id = self.k_list[0][0], 
                                                 destination_address = self.k_list[0][1])
        if recv_json_k_list is sock_req.error_json: return 

        self.succ_list = [self.succ_list[0]] + recv_json_k_list['return_info']["k_list"][:-1]

        if self.between(recv_json_predecessor['return_info']['predeccesor_id'], interval = (self.id, self.k_list[0][0]) ):
            
            recv_json_pred_k_list = sock_req.make_request( json_to_send = {"command_name" : "get_k_list", 
                                                                              "method_params" : {}, 
                                                                              "procedence_address" : self.address, 
                                                                              "procedence_method":  "stabilize"}, 
                                                             requester_object = self, 
                                                             asked_properties = ('k_list',), 
                                                             destination_id = recv_json_predecessor['return_info'][ 'predecessor_id'], 
                                                             destination_address = recv_json_predecessor['return_info'][ 'predecessor_address'])
            if not recv_json_pred_k_list is sock_req.error_json:
                
                #If it's true that self has a new succesor and this new sucstabcesor is alive, then self has to actualize its succ_list    
                self.k_list = [[recv_json_predecessor['return_info']['predecessor_id'], 
                                recv_json_predecessor['return_info']['predecessor_address']]] + recv_json_pred_k_list['return_info']['k_list'][:-1]                                       
            else:
                self.verbose_option: sock_req.action_for_error(recv_json_predecessor['return_info'][ 'predeccesor_address'])

    def command_rect(self, predecessor_id, predecessor_address, sock_req):
        
        if self.between(predecessor_id, interval = (self.predecessor_id, self.id)) or self.id == self.predecessor_id:
            
            if self.predecessor_id == self.id: 

                self.k_list[0] = (predecessor_id, predecessor_address)                
            self.predecessor_id, self.predecessor_address = predecessor_id, predecessor_address

        else:
                        
            recv_json_alive = sock_req.make_request(json_to_send = {"command_name" : "are_you_alive", 
                                                                    "method_params" : {}, 
                                                                    "procedence_address" : self.address, 
                                                                    "procedence_method": "rect"}, 
                                                    destination_id = self.predecessor_id, 
                                                    destination_address = self.predecessor_address)
            
            if recv_json_alive is sock_req.error_json:
                sock_req.action_for_error(self.predecessor_address)   
                self.predecessor_id, self.predecessor_address = predecessor_id, predecessor_address             
                sock_req.action_for_error(self.predecessor_address)
        
        self.sock_rep.send_json( { "response": "ACK" } )


    def waiting_for_commands(self, client_request):        
        self.sock_rep = self.context.socket(zmq.REP)
        self.sock_rep.bind("tcp://" + self.address)    
                
        while True:

            print("Waiting "+ str(self.finger_table)+" - yo "+ str(self.address)+", "+ str(self.id) + " predecesor: "+ str(self.predecessor_address)+", "+ str(self.predecessor_id) )
            buffer = self.sock_rep.recv_json()

            if buffer['command_name'] in self.commands:
                
                print(buffer)
                if buffer['command_name'] in self.commands_request:
                    print("comando con request: " + str(buffer['command_name']))
                    self.commands[buffer["command_name"]](**buffer["method_params"], sock_req = client_request)
                else:
                    print("comando sin request: " + str(buffer['command_name']))
                    self.commands[buffer["command_name"]](**buffer["method_params"])
                    
        self.sock_rep.close()      
        
    def command_find_successor(self, id, sock_req):
        info = self.find_successor(id, sock_req)
        self.sock_rep.send_json({"response": "ACK", "return_info": info})
        pass


    def find_successor(self, id, sock_req):
        tuple_info = self.find_predecessor(id, sock_req)
        if tuple_info:
            destination_id, destination_address = tuple_info            
            recv_json = sock_req.make_request(json_to_send = {"command_name" : "get_k_list", 
                                                              "method_params": {}, 
                                                              "procedence_address": self.address, 
                                                              "procedence_method": "find_successor"}, 
                                              requester_object= self, 
                                              asked_properties = ('k_list', ), 
                                              destination_id = destination_id, 
                                              destination_address = destination_address ) 
            if recv_json is sock_req.error_json: return None
            return recv_json['return_info']['k_list'][0]
        return None
        
    def command_get_params(self):  
        print("entro comando get params")      
        self.sock_rep.send_json({"response": "ACK", 
                                 "return_info": {"finger_table" : self.finger_table, 
                                                 "predecessor_address" : self.predecessor_address, 
                                                 "predecessor_id" : self.predecessor_id, 
                                                 "k_list" : self.k_list, 
                                                 "id": self.id, 
                                                 "address": self.address } })

    def command_get_prop(self, prop_name):
        print("entro comando get prop")
        if prop_name == "start_indexes":
            self.sock_rep.send_json({'response': "ACK", 
                                     "return_info" : [self.start(i) for i in range(self.size)] })    

        self.sock_rep.send_json({'response': 'ACK', "return_info": self.__dict__[prop_name] })
        
    def command_get_predecessor(self):
        print("entro comando get predecessor")
        self.sock_rep.send_json({"response": "ACK", 
                                 "return_info": {"predecessor_id" : self.predecessor_id, 
                                                 "predecessor_address" : self.predecessor_address } } )
        
    
    def command_are_you_alive(self):
        print("entro comando are you alive")
        self.sock_rep.send_json({"response": "ACK", "procedence_addr": self.address})
    
    
    
    
    
    

    def command_send_file(self, path):
        # Verify that the file is available
        if not os.path.isfile("data/" + path):
            self.sock_rep.send('')
            return
        print("Existe el archivo")
        # Open the file for reading
        fn = open("data/" + path, 'rb')
        stream = True
        print("Enviando data")
        # Start reading in the file
        while stream:
            # Read the file bit by bit
            stream = fn.read(128)
            if stream:
                # If the stream has more to send then send more
                self.sock_rep.send(stream, zmq.SNDMORE)
            else:
                # Finish it off
                self.sock_rep.send(stream)
        
        

    def command_recv_file(self, path, destination_address):
        self.sock_rep.send_json({})   
        
        # Open up the file we are going to write to
        dest = open("data/" + os.path.basename(path), 'wb')
        socket_request = self.context.socket(zmq.REQ)
        for i in range(3):
            socket_request.connect('tcp://' + destination_address)
            print('tcp://' + destination_address)
            # send the desired file to the server
            socket_request.send(path.encode())
            print("hola again")

            if socket_request.poll(1000):
                while True:
                    print("oinmp")
                    # Start grabing data
                    data = socket_request.recv()
                    # Write the chunk to the file
                    dest.write(data)
                    if not socket_request.getsockopt(zmq.RCVMORE):
                        # If there is not more data to send, then break
                        break
                socket_request.disconnect("tcp://" + str(destination_address)) 
                return
            socket_request.disconnect("tcp://" + destination_address)
            socket_request.setsockopt(zmq.LINGER, 0)
            socket_request.close()
            
            socket_request = self.context.socket(zmq.REQ)
              



        
    def command_update_predecessor(self, predecessor_id, predecessor_address):
        print("entro comando get predecessor")
        self.predecessor_id = predecessor_id
        self.predecessor_address = predecessor_address
        self.sock_rep.send_json({"response": "ACK", "return_info": {}})

    def command_get_successor(self, x):
        print("entro comando get successor")
        id, address = self.finger_table[x-1][0],self.finger_table[x-1][1]
        self.sock_rep.send_json({"response": "ACK", "return_info": {"successor_pos_x_id": id, "successor_pos_x_address": address}})

    
    

    """ def thread_verify (self):
        print("entro thread verify")
        rand = Random()
        requester = request(context = self.context)
        while True:
            to_sleep= rand.randint(self.waiting_time[0],self.waiting_time[1])
            print("esperar por " + str(to_sleep))
            sleep(to_sleep)
            if self.predecessor_id != self.id:
                self.verify_alive_nodes(sock_req = requester) 
                print("paso command verify alive nodes") """              

             
    """ # Verificar si los nodos de la finger table siguen vivos
    def verify_alive_nodes(self, sock_req : request):
        if len(self.finger_table) == 0:
            self.sock_rep.send_json({"response": "ACK_verify_alive_nodes", "return_info": {}})
            return                        
        
        # Cantidad de nodos muertos al principio de mi finger table
        losses_node = 0
        
        for i in range(len(self.finger_table)):
            print("Verificando si el nodo " + str(self.finger_table[i][0]) + " está vivo")
            # Verificar si el nodo esta vivo
            recv_json_alive = sock_req.make_request(
                json_to_send = {"command_name" : "are_you_alive", "method_params" : {}, "procedence_addr" : self.address, "procedence_method": "verify_alive_nodes"},  
                destination_address = self.finger_table[i][1])
            # Si da error es porque nunca contestó y se asume que está muerto
            if recv_json_alive is sock_req.error:
                sock_req.action_for_error(self.finger_table[i][1])
                print("Hemos perdido a " + str(self.finger_table[i][0]) + ", actuaremos en consecuencia")
                losses_node += 1
            elif losses_node ==0:
                print("Todo OK")
                return
                
        if len(self.finger_table)-losses_node==0:
            print("Me quede solo")
            self.predecessor_address=None
            self.predecessor_id=None
            self.finger_table = []
            return
        
        print("Arregla chord el sig nodo: " + str(self.finger_table[losses_node][1]))
        # Arregla
        recv_json_update = sock_req.make_request(
        json_to_send = {"command_name" : "nodes_losses_fix_chord", 
                        "method_params": {"predecessor_id": self.id,
                                            "predecessor_address": self.address,
                                            "nodes_losses": losses_node}, 
                        "procedence_addr": self.address, "procedence_method": "verify_alive_nodes"}, 
        destination_address = self.finger_table[losses_node][1])
        if recv_json_update is sock_req.error:
                sock_req.action_for_error(self.finger_table[losses_node][1])
        
        
    # Arreglar el chord
    def nodes_losses_fix_chord(self, predecessor_id, predecessor_address, nodes_losses, sock_req : request):
        print("Cambie mis predecesores")
        self.predecessor_id = predecessor_id
        self.predecessor_address = predecessor_address
        if len(self.finger_table) < self.finger_table_length:
            for _ in range(nodes_losses):  self.finger_table.pop()
        else:
            for i in range(nodes_losses):
                # Verificar si el nodo esta vivo
                recv_json_alive = sock_req.make_request(
                    json_to_send = {"command_name" : "are_you_alive", "method_params" : {}, "procedence_addr" : self.address, "procedence_method": "verify_alive_nodes"},  
                    destination_address = self.finger_table[self.finger_table_length-1-i][1])
                if recv_json_alive is sock_req.error:
                    sock_req.action_for_error(self.predecessor_address)      
                    self.finger_table.pop()
                else:
                    break
                    
        print("Mi nueva finger table: " + str(self.finger_table))
        
        new_finger_table = self.finger_table.copy()
        new_finger_table.pop()
        new_finger_table.insert(0, (self.id, self.address))
        print("Finger table para predecesor: " + str(new_finger_table))
         
        recv_json_update_predecessor = sock_req.make_request(
                json_to_send = {"command_name" : "replace_finger_table_consecutive", 
                                "method_params": {"finger_table": new_finger_table,
                                                "iterations": len(new_finger_table)-1}, 
                                "procedence_addr": self.address, "procedence_method": "nodes_losses_fix_chord"}, 
                destination_address = self.predecessor_address)
        
        if recv_json_update_predecessor is sock_req.error:
                sock_req.action_for_error(self.predecessor_address)
        self.sock_rep.send_json({"response": "ACK_to_fix_chord", "return_info": {}})
     """
    """      
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
    """
    """ 
    # Método para calcular el id que debe tener en el chord una pc que desea entrar
    def calculate_id_in(self, address_request, initial_id, best_id, best_address_to_in, best_score, auto_activation, sock_req : request):
        
        if not auto_activation:
            self.sock_rep.send_json({"response": "ACK_to_calculate_id", "return_info": {}})
        
        print("Buscando la mejor posición para el nuevo nodo...")
        #La idea es calcular el mayor espaciamiento entre dos nodos en el chord
        # best_score inicia en 1 para que no se elijan nodos consecutivos

        if initial_id == None:  # Si es none es porque este es el primer request
            initial_id = self.id
        
        if len(self.finger_table) == 0: # Si solo hay un nodo en el chord
            if self.id < self.size - self.id:
                best_id = self.size + self.id
            else:
                best_id = self.id

            if best_id % 2 == 0:
                best_id = int(best_id / 2)
            else:
                best_id = int(best_id / 2) + 1
            self.get_in_new_node(address_request, best_id, True, sock_req)
            return 0



        # Primero calculamos el espaciamiento entre esta pc y la siguiente
        
        successor_id = self.finger_table[0][0]

        if self.id < successor_id:
            if successor_id - self.id > best_score:
                best_score = successor_id - self.id
                best_id = self.id + successor_id
                if best_id % 2 == 0:
                    best_id = int(best_id / 2)
                else:
                    best_id = int(best_id / 2) + 1
                best_address_to_in = self.address
        else:
            if self.size - self.id + successor_id > best_score:
                best_score = self.size - self.id + successor_id

                best_id = self.id + self.size + successor_id
                
                if best_id % 2 == 0:
                    best_id = int(best_id / 2)
                else:
                    best_id = int(best_id / 2) + 1

                if best_id >= self.size:
                    best_id = best_id - self.size
                best_address_to_in = self.address

        # Luego revisamos todos los espaciamientos en la finger table

        for i in range(len(self.finger_table) - 1):

            first_id = self.finger_table[i][0]
            second_id = self.finger_table[i + 1][0]

            if self.finger_table[i][0] == initial_id:
                if self.address == best_address_to_in:
                    self.get_in_new_node(address_request, best_id, True, sock_req)
                else:
                    # Ya con esto recorrimos todo el chord y encontramos el mejor espaciamiento
                    # Ahora le enviamos ese mensaje al nodo designado para que reciba al nuevo nodo
                    recv_json = sock_req.make_request(json_to_send = {"command_name" : "get_in_new_node", 
                                                                    "method_params" : {"address_to_get_in": address_request,
                                                                                        "id_to_place": best_id,
                                                                                        "auto_activation" : False}},
                                            requester_object = self,
                                            destination_address = best_address_to_in)
                return 0



            if  first_id < second_id:
                if  second_id - first_id > best_score:
                    best_score = second_id - first_id
                    best_id = first_id + second_id
                    if best_id % 2 == 0:
                        best_id = int(best_id / 2)
                    else:
                        best_id = int(best_id / 2) + 1
                    best_address_to_in = self.finger_table[i][1]
            else:
                if self.size - first_id + second_id > best_score:
                    best_score = self.size - first_id + second_id

                    best_id = first_id + self.size + second_id
                    
                    if best_id % 2 == 0:
                        best_id = int(best_id / 2)
                    else:
                        best_id = int(best_id / 2) + 1

                    if best_id >= self.size:
                        best_id = best_id - self.size
                    best_address_to_in = self.finger_table[i][1]
             
        if self.finger_table[len(self.finger_table) - 1][0] == initial_id:
            print("Ya le di la vuelta a la finger table")
            if self.address == best_address_to_in:
                print("Tengo que entrarlo yo " + str(self.id))
                self.get_in_new_node(address_request, best_id, True, sock_req)
                
            else:
                # Ya con esto recorrimos todo el chord y encontramos el mejor espaciamiento
                # Ahora le enviamos ese mensaje al nodo designado para que reciba al nuevo nodo
                print("Tiene que entrarlo el otro nodo")
                recv_json = sock_req.make_request(json_to_send = {"command_name" : "get_in_new_node", 
                                                                "method_params" : {"address_to_get_in": address_request,
                                                                                    "id_to_place": best_id,
                                                                                    "auto_activation" : False}},
                                        requester_object = self,
                                        destination_address = best_address_to_in)
        else:
            # Enviar todos los datos actuales al nodo en la última posición de la finger table
            recv_json = sock_req.make_request(json_to_send = {"command_name" : "calculate_id_in", 
                                                                        "method_params" : {"address_request": address_request,
                                                                                            "initial_id" : initial_id, 
                                                                                            "best_id" : best_id, 
                                                                                            "best_address_to_in" : best_address_to_in,
                                                                                            "best_score" : best_score,
                                                                                            "auto_activation" : False}},
                                                requester_object = self,
                                                destination_address = self.finger_table[len(self.finger_table) - 1][1])


    # Método para que un nodo entre a otro como su sucesor en el chord
    def get_in_new_node(self, address_to_get_in, id_to_place, auto_activation, sock_req : request):
       
        if not auto_activation: 
            self.sock_rep.send_json({"response": "ACK_to_get_new_node", "return_info": {}})
        print("Ya es posible entrar al nuevo nodo")
        # Primero le damos nuestra finger table para que actualice la suya
        recv_json = sock_req.make_request(json_to_send = {"command_name" : "get_in_to_chord_succefully", 
                                                          "method_params" : {"new_id" : id_to_place,
                                                                             "finger_table" : self.finger_table,
                                                                             "predecessor_address" : self.address,
                                                                             "predecessor_id" : self.id}},
                                          requester_object = self,
                                          destination_address = address_to_get_in)

        if len(self.finger_table) > 0:
            #Luego actualizamos el antecesor del  sucesor del nuevo nodo
            recv_json = sock_req.make_request(json_to_send = {"command_name" : "replace_predeccessor", 
                                                              "method_params" : {"new_predecessor_address" : address_to_get_in,
                                                                                 "new_predecessor_id": id_to_place}},
                                             requester_object = self,
                                             destination_address = self.finger_table[0][1])
        else:
            self.predecessor_address = address_to_get_in
            self.predecessor_id = id_to_place

        # Luego modificamos la finger table del nodo actual
        if len(self.finger_table) == self.finger_table_length:
            self.finger_table = self.finger_table[:len(self.finger_table) - 1]
        self.finger_table.insert(0, [id_to_place, address_to_get_in])
        
        print(self.finger_table)
        print(self.predecessor_address)

        finger_table_to_send = self.finger_table.copy()
        if len(finger_table_to_send) < self.finger_table_length:
            finger_table_to_send = finger_table_to_send[:len(finger_table_to_send) - 1]
        finger_table_to_send.insert(0, [self.id, self.address])

        # Y comenzamos a avisarle al resto de nodos anteriores de que actualicen su finger table
        recv_json = sock_req.make_request(json_to_send = {"command_name" : "replace_finger_table_consecutive", 
                                                            "method_params" : {"finger_table": finger_table_to_send,
                                                                                "iterations": len(self.finger_table)}},
                                        requester_object = self,
                                        destination_address = self.predecessor_address)

    def get_in_to_chord_succefully(self, new_id, finger_table, predecessor_address, predecessor_id):
        print("Entré al chord satisfactoriamente")
        self.id = new_id
        self.finger_table = finger_table
        self.predecessor_address = predecessor_address
        self.predecessor_id = predecessor_id
        self.sock_rep.send_json({"response": "ACK_to_get_in_sucefully", "return_info": {}})


    def replace_predeccessor(self, new_predecessor_address, new_predecessor_id):
        print("Voy a reemplazar a mi predecesor porque algo pasó")
        self.predecessor_id = new_predecessor_id
        self.predecessor_address = new_predecessor_address
        self.sock_rep.send_json({"response": "ACK_to_replace_predecessor", "return_info": {}})


    # Método para estabilizar las finger table de los nodos anteriores
    def replace_finger_table_consecutive(self, finger_table, iterations, sock_req: request):
        print("Voy a estabilizar mi finger table porque algo pasó" + str(finger_table))
        
        for i in range(len(finger_table)):
            if (self.id, self.address) == finger_table[i]:
                del(finger_table[i])
                break
            
        self.finger_table = finger_table
        if iterations > 0:
            finger_table_to_send = self.finger_table.copy()
            if len(finger_table_to_send) < self.finger_table_length:
                finger_table_to_send = finger_table_to_send[:len(finger_table_to_send) - 1]
            finger_table_to_send.insert(0, [self.id, self.address])

            # Y comenzamos a avisarle al resto de nodos anteriores de que actualicen su finger table
            recv_json = sock_req.make_request(json_to_send = {"command_name" : "replace_finger_table_consecutive", 
                                                                "method_params" : {"finger_table": finger_table_to_send,
                                                                                    "iterations": iterations - 1}},
                                            requester_object = self,
                                            destination_address = self.predecessor_address)
        self.sock_rep.send_json({"response": "ACK_to_replace_finger_table", "return_info": {}}) """