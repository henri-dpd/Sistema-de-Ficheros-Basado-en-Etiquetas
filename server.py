
from functools import reduce
import hashlib
import os
from random import Random
import math
import re
from time import time, sleep
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
        self.k_list = [(self.id, self.address) for _ in range(self.k)]
        self.finger_table_length = int(math.log2(self.size))
        self.finger_table = [None for _ in range(self.size)]
        self.waiting_time_stabilize = 3
        self.waiting_time_fix_finger = 0.5
        self.waiting_time_repl = 10
        self.is_leader = False
        self.hash_tags = {} # tag_id: {objects_id: objetc_path}
        self.replication = {"id" : None, "tags" : {}}
        
        self.commands = {"join": self.command_join, 
                         "are_you_alive": self.command_are_you_alive,
                         "get_params": self.command_get_params, 
                         "get_prop": self.command_get_prop,
                         "get_predecessor": self.command_get_predecessor,
                         "update_predecessor": self.command_update_predecessor,
                         "get_successor": self. command_get_successor,
                         "find_successor": self.command_find_successor,
                         "find_predecessor": self.command_find_predecessor,
                         "recv_file" : self.command_recv_file,
                         "send_file" : self.command_send_file,
                         "closest_predecessor_fing" : self.command_closest_predecessor_fing,
                         "get_k_list" : self.command_get_k_list,
                         "rect" : self.command_rect,
                         "stabilize" : self.command_stabilize,
                         "recv_tag" : self.command_recv_tag,
                         "get_tag": self.command_get_tag,
                         "get_object" : self.send_file,
                         "cut_object" : self.cut_file,
                         "send_files_and_tag_for_new_node" : self.send_files_and_tag_for_new_node,
                         "get_files_for_replication": self.get_files_for_replication,
                         "send_files_for_replication": self.send_files_for_replication,
                         "get_tag_for_replication": self.get_tag_for_replication,
                         "send_tags_for_replication" : self.send_tags_for_replication,
                         "delete_object_from_replication" : self.delete_object_from_replication,
                         "delete_tags_from_replication" : self.delete_tags_from_replication
                         }

        self.commands_request = {"rect", "stabilize", "find_successor", "find_predecessor", 
                                 "closest_predecessor_fing", "recv_file","get_tag", "send_file",
                                 "send_files_and_tag_for_new_node", "send_files_for_replication",
                                 "recv_tag", "cut_object"}
        
        try:
            os.mkdir("data")
        except:
            pass
        
        try:
            os.mkdir("data/" + str(self.id))
        except:
            pass
        
        os.system("rm data/" + str(self.id) + "/*")
        
        try:
            os.mkdir("data/" + str(self.id) + "/Replication" )
        except:
            pass
        
        os.system("rm data/" + str(self.id) + "/Replication/*")

        print("Started node ", (self.id, self.address))
        client_requester = request(context = self.context)
        if introduction_node:
            introduction_node_id = self.get_node_hash(introduction_node)
            recieved_json = client_requester.make_request(json_to_send = {"command_name" : "join",
                                                                         "method_params" : {}, 
                                                                         "procedence_address" : self.address}, 
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
                                                                         "procedence_address" : self.address}, 
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
                                                                         "procedence_address" : self.address}, 
                                                          destination_address = introduction_node,
                                                          destination_id = introduction_node_id)
            
            recieved_json = client_requester.make_request(json_to_send = {"command_name" : "find_successor",
                                                                         "method_params" : {"id": self.id}, 
                                                                         "procedence_address" : self.address}, 
                                                          destination_address = self.predecessor_address,
                                                          destination_id = self.predecessor_id)
            
            actual_successor = recieved_json["return_info"]
            
            recieved_json = client_requester.make_request(json_to_send = {"command_name" : "send_files_and_tag_for_new_node",
                                                                         "method_params" : {"id": self.id}, 
                                                                         "procedence_address" : self.address}, 
                                                          destination_address = actual_successor[1],
                                                           destination_id = actual_successor[0])

            
            list_of_tags_recieved = recieved_json["return_info"]["list_of_tags_to_send"]
            list_of_file_recieved = recieved_json["return_info"]["list_of_file_to_send"]
            
            for tag in list_of_tags_recieved:
                if int(tag) in self.hash_tags:
                    for path_id in tag:
                        self.hash_tags[int(tag)][int(path_id)] = list_of_tags_recieved[tag][path_id]
                else:
                    self.hash_tags[int(tag)] = list_of_tags_recieved[tag]
            
            self.try_to_get_files(list_of_file_recieved, actual_successor)    
                       
        else:
            self.predecessor_address, self.predecessor_id = self.address, self.id
            self.is_leader = True
        self.execute(client_requester)
        
    def try_to_get_files(self, list_of_file_recieved, actual_successor):    
        for filename in list_of_file_recieved:
            print(filename)
            socket_request = self.context.socket(zmq.REQ)
            socket_request.connect('tcp://' + str(actual_successor[1]))
            print('tcp://' + actual_successor[1])
            
            
            dest = open("data/" + str(self.id) + "/" + filename, 'wb')
            
            socket_request.send_json({"command_name": "cut_object",
                                        "method_params": {"path" : filename}, 
                                        "procedence_addr": self.address})

            print("Comenzar a leer")
            while True:
                # Comenzamos a recibir la data
                data = socket_request.recv()
                # escribe en el fichero abierto
                dest.write(data)
                if not socket_request.getsockopt(zmq.RCVMORE):
                    break
            
            print("Termino de leer")
            socket_request.disconnect("tcp://" + str(actual_successor[1]))
            socket_request.close()
            
        
    def get_node_hash(self, address):
        summ = ''
        for x in address.split(":")[0].split(".") + [address.split(":")[1]]:
            summ += x
        return int(hashlib.sha1(bytes(summ, 'utf-8') ).hexdigest(),16)

    def start(self, i):
        return (self.id + 2**(i)) % 2**self.size


    def command_join(self):        
        self.sock_rep.send_json({"response": "ACK_to_join", "return_info": {}})

    def execute_join(self, introduction_node, introduction_node_id, id_to_found_pred, sock_req):
        recv_json = sock_req.make_request(json_to_send = {"command_name" : "find_predecessor",
                                                          "method_params" : {"id" : id_to_found_pred},
                                                          "procedence_address" : self.address},
                                          requester_object = self,
                                          method_for_wrap = "find_predecessor",
                                          destination_id = introduction_node_id,
                                          destination_address = introduction_node)
        if recv_json is sock_req.error:
            return False
        
        self.predecessor_id, self.predecessor_address = recv_json['return_info']['predecessor_id'], recv_json['return_info']['predecessor_address']
        recv_json = sock_req.make_request(json_to_send = {"command_name" : "get_k_list", 
                                                          "method_params" : {}, 
                                                          "procedence_address" : self.address}, 
                                          requester_object = self, 
                                          asked_properties = ("k_list",), 
                                          destination_id = recv_json['return_info']['predecessor_id'], 
                                          destination_address = recv_json['return_info']['predecessor_address'] )         
        if recv_json is sock_req.error:
            return False
        self.k_list = recv_json['return_info']['k_list']
        return True
    
    def send_files_and_tag_for_new_node(self, id, sock_req):
        
        os.system("ls ./data/" + str(self.id) + " > data/" + str(self.id) + "/temp.txt")
        temp_file = open("data/" + str(self.id) + "/temp.txt", 'r')
        
        list_of_files = temp_file.read().split("\n")
        list_of_file_to_send = []
        
        for filename in list_of_files:
            if filename != "temp.txt" and filename != "" and filename != "Replication":
                file_id = int(hashlib.sha1(bytes(filename, 'utf-8') ).hexdigest(),16)
                if file_id <= id:
                    
                    if self.id < id and file_id <= self.id:
                        continue # Caso esquinado donde self.id < id, en
                                 # este caso solo se entregan los files
                                 # entre el id actual y el que se pide
                    
                    list_of_file_to_send.append(filename)
                    
        list_of_tags_to_send = {}
        
        for tag_id in self.hash_tags:
            if tag_id <= id:
                
                if self.id < id and tag_id <= self.id:
                        continue # Caso esquinado donde self.id < id, en
                                 # este caso solo se entregan las etiquetas
                                 # entre el id actual y el que se pide
                
                list_of_tags_to_send[tag_id] = self.hash_tags[tag_id]
                
        for tag_id in list_of_tags_to_send:
            del(self.hash_tags[tag_id])
                
        os.system("rm data/" + str(self.id) + "/temp.txt")
        self.sock_rep.send_json({"response": "ACK", "return_info": {"list_of_tags_to_send" : list_of_tags_to_send,
                                                                    "list_of_file_to_send" : list_of_file_to_send},
                                 "procedence_addr": self.address })
        
        successor = self.finger_table[0]
        
        recv_json = sock_req.make_request(json_to_send = {'command_name': 'delete_tags_from_replication', 
                                                                'method_params': {'tags': list_of_tags_to_send}}, 
                                                requester_object= self, 
                                                destination_id = successor[0],
                                                destination_address = successor[1])
        
    def delete_tags_from_replication(self, tags):
        self.sock_rep.send_json({})
        for tag in tags:
            del(self.replication["tags"][int(tag)])


    def command_find_predecessor(self, id, sock_req):
        print("entro comando find predecessor")
        predecessor_id, predecessor_address = self.find_predecessor(id, sock_req)

        self.sock_rep.send_json({"response": "ACK", "return_info": {"predecessor_id": predecessor_id, "predecessor_address": predecessor_address}, "procedence_addr": self.address } )
        
        
    def find_predecessor(self, id, sock_req):
        current_id = self.id
        current_k_list_id = self.k_list[0][0]
        self.finger_table[0] = self.k_list[0]
        current_address = self.address  
        while not self.between(id, interval = (current_id, current_k_list_id)) and current_k_list_id != id :
            recv_json_closest_predecessor = sock_req.make_request(json_to_send = {"command_name" : "closest_predecessor_fing",
                                                                                  "method_params" : {"id": id}, 
                                                                                  "procedence_address" : self.address, 
                                                                                  "procedence_method": "find_predecessor"}, 
                                                                  method_for_wrap = 'closest_predecessor_fing', 
                                                                  requester_object = self, 
                                                                  destination_id = current_id, 
                                                                  destination_address = current_address)
            if recv_json_closest_predecessor is sock_req.error : 
                return None
            recv_json_k_list = sock_req.make_request(json_to_send = {"command_name" : "get_k_list", 
                                                                   "method_params" : {}, 
                                                                   "procedence_address" : self.address, 
                                                                   "procedence_method" : "find_predecessor" }, 
                                                   requester_object = self, 
                                                   asked_properties = ("k_list", ), 
                                                   destination_id = recv_json_closest_predecessor['return_info'][0], 
                                                   destination_address = recv_json_closest_predecessor['return_info'][1] )
            if recv_json_k_list is sock_req.error:
                return None
            current_id, current_address = recv_json_closest_predecessor['return_info'][0], recv_json_closest_predecessor['return_info'][1]
            current_k_list_id = recv_json_k_list['return_info']['k_list'][0][0]               
        
        return current_id, current_address

    def command_closest_predecessor_fing (self, id, sock_req):        
        closest_id, closest_address = self.closest_predecessor_fing(id, sock_req)
        self.sock_rep.send_json({"response" : "ACK", "return_info" : (closest_id, closest_address), "procedence": self.address})


    def closest_predecessor_fing(self, id, sock_req):
        for i in range(self.size-1, -1, -1):            
            if self.finger_table[i] is None : continue 
            if self.between(self.finger_table[i][0], (self.id, id) ) :
                return self.finger_table[i]                
        return (self.id, self.address)

    def between(self, id, interval):
        if interval == None or id == None:
            return None
        
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
        countdown_stabilize = time()
        countdown_fix_finger = time()
        countdown_repl = time()
        rand = Random()
        rand.seed()
        requester = request(context = self.context)
        choices = [i for i in range(self.size)]
        while True:
            
            if abs (countdown_fix_finger - time( ) ) > self.waiting_time_fix_finger:
                if self.predecessor_id != self.id:
                    index = rand.choice( choices )   
                    self.finger_table[ index ] = self.find_successor(self.start(index), sock_req = requester)
                countdown_fix_finger = time()
            
            if abs (countdown_stabilize - time( ) ) > self.waiting_time_stabilize:
                if self.predecessor_id != self.id:
                    self.command_stabilize(sock_req = requester)
                    if requester.make_request(json_to_send = {"command_name" : "rect", 
                                                              "method_params" : { "predecessor_id": self.id, 
                                                                                 "predecessor_address" : self.address }, 
                                                              "procedence_address" : self.address, 
                                                              "procedence_method": "thread_verify", 
                                                              "time": time()}, 
                                              destination_id = self.k_list[0][0], 
                                              destination_address = self.k_list[0][1]) is requester.error:
                        requester.action_for_error(self.k_list[0][1])
                countdown_stabilize = time()
            
            
            if abs (countdown_repl - time( ) ) > self.waiting_time_repl:                
                if self.predecessor_id != self.id:
                    print("Hay mas de un nodo\n")
                    
                    actual_successor = self.finger_table[0]
                        
                    if(actual_successor[0] != self.predecessor_id):
                        
                        print("Hay mas de dos nodos\n")    
                    
                        if self.replication["id"] != self.predecessor_id:
                            
                            print("Algo cambio, es necesario replicar\n")
                            
                            # Si el nodo al que replicábamos cambio, puede haber ocurrido
                            # una de dos cosas, o desapareció, lo cual indica que ahora el id
                            # del predecesor es menor que el que tenemos en Replication,
                            # o se agregó uno en medio, el cuál tendrá mayor id que el que
                            # estábamos replicando
                            if (self.replication["id"] == None or
                                self.between(self.predecessor_id, (self.replication["id"], self.id))):
                                
                                print("Un nodo fue agregado, replicando...\n")
                                self.replication = {"id" : None, "tags" : {}}
                                try:
                                    os.system("rm data/" + str(self.id) + "/Replication/*")
                                except:
                                    print("No se pudo o hubo ningun directorio para remover")
                            
                            else:
                                print("Un nodo fue eliminado, replicando...\n")
                                
                                os.system("ls ./data/" + str(self.id) + "/Replication > data/" + str(self.id) + "/replication_temp.txt")
                                replication_temp_file = open("data/" + str(self.id) + "/replication_temp.txt", 'r')
                                list_of_files = replication_temp_file.read().split("\n")
                                                            
                                try:
                                    os.system("mv data/" + str(self.id) + "/Replication/* data/" + str(self.id))
                                except:
                                    print("No fue posible mover los datos o no habian datos para mover")
                                    
                                pos = []
                                
                                for i in range(len(list_of_files)):
                                    if (list_of_files[i] == "Replication" or
                                        list_of_files[i] == "" or
                                        list_of_files[i] == "replication_to_send_temp.txt" or
                                        list_of_files[i] == "replication_temp.txt"):
                                        
                                        pos.append(i)
                                        
                                for i in range(len(pos) - 1, -1, -1):
                                    del(list_of_files[pos[i]])
                                
                                recv_json = requester.make_request(json_to_send = {"command_name" : "get_files_for_replication", 
                                                                        "method_params" : {"list_files" : list_of_files,
                                                                                        "destination_address" : self.address}, 
                                                                        "procedence_address" : self.address}, 
                                                        destination_id = actual_successor[0], 
                                                        destination_address = actual_successor[1])
                                
                                
                                recv_json = requester.make_request(json_to_send = {"command_name" : "send_tags_for_replication", 
                                                                        "method_params" : {"list_tags" : self.replication["tags"]}, 
                                                                        "procedence_address" : self.address}, 
                                                        destination_id = actual_successor[0], 
                                                        destination_address = actual_successor[1])
                                
                                
                                for tag in self.replication["tags"]:
                                    if tag in self.hash_tags:
                                        for path_id in tag:
                                            self.hash_tags[tag][path_id] = self.replication["tags"][tag][path_id]
                                    else:
                                        self.hash_tags[tag] = self.replication["tags"][tag]
                                
                                    
                                self.replication["tags"] = {}
                                
                                try:
                                    os.system("rm data/" + str(self.id) + "/replication_temp.txt")
                                except:
                                    print("No se pudo o hubo ningun directorio para remover")
                            
                            
                            recv_json = requester.make_request(json_to_send = {"command_name" : "send_files_for_replication", 
                                                                    "method_params" : {}, 
                                                                    "procedence_address" : self.address}, 
                                                    destination_id = self.predecessor_id, 
                                                    destination_address = self.predecessor_address)
                            
                            
                            recv_json = requester.make_request(json_to_send = {"command_name" : "get_tag_for_replication", 
                                                                    "method_params" : {},
                                                                    "procedence_address" : self.address}, 
                                                    destination_id = self.predecessor_id, 
                                                    destination_address = self.predecessor_address)
                            
                            self.replication["id"] = self.predecessor_id
                            self.replication["tags"] = recv_json["return_info"]["tags"]
                            

                            print(recv_json)
                            
                            
                            print("Replicación satisfactoria. \n")
                        else:
                            print("No fue necesario Replicar. \n")
                
                print(self.replication)
                
                countdown_repl = time()

    def command_stabilize(self, sock_req : request):
        print("Stabilize")                     
        recv_json_predecessor = sock_req.make_request(json_to_send = {"command_name" : "get_predecessor", 
                                                               "method_params" : {}, 
                                                               "procedence_address" : self.address, 
                                                               "procedence_method": "stabilize"}, 
                                               requester_object = self, 
                                               asked_properties = ('predecessor_id', 'predecessor_address'), 
                                               destination_id = self.k_list[0][0], 
                                               destination_address = self.k_list[0][1])
        if recv_json_predecessor is sock_req.error:
            sock_req.action_for_error(self.k_list[0][1])
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
        if recv_json_k_list is sock_req.error: return 

        self.k_list = [self.k_list[0]] + recv_json_k_list['return_info']["k_list"][:-1]

        if self.between(recv_json_predecessor['return_info']['predecessor_id'], interval = (self.id, self.k_list[0][0]) ):
            
            recv_json_pred_k_list = sock_req.make_request( json_to_send = {"command_name" : "get_k_list", 
                                                                              "method_params" : {}, 
                                                                              "procedence_address" : self.address, 
                                                                              "procedence_method":  "stabilize"}, 
                                                             requester_object = self, 
                                                             asked_properties = ('k_list',), 
                                                             destination_id = recv_json_predecessor['return_info'][ 'predecessor_id'], 
                                                             destination_address = recv_json_predecessor['return_info'][ 'predecessor_address'])
            if not recv_json_pred_k_list is sock_req.error:
                
                self.k_list = [[recv_json_predecessor['return_info']['predecessor_id'], 
                                recv_json_predecessor['return_info']['predecessor_address']]] + recv_json_pred_k_list['return_info']['k_list'][:-1]                                       
            else:
                sock_req.action_for_error(recv_json_predecessor['return_info'][ 'predecessor_address'])

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
            
            if recv_json_alive is sock_req.error:
                sock_req.action_for_error(self.predecessor_address)   
                self.predecessor_id, self.predecessor_address = predecessor_id, predecessor_address             
                sock_req.action_for_error(self.predecessor_address)
        
        self.sock_rep.send_json( { "response": "ACK" } )


    def waiting_for_commands(self, client_request):        
        self.sock_rep = self.context.socket(zmq.REP)
        self.sock_rep.bind("tcp://" + self.address)    
                
        while True:

            print("Waiting "+ str(self.address)+", "+ str(self.id) + " predecessor: "+ str(self.predecessor_address)+", "+ str(self.predecessor_id) )
            print("tags: "+str(self.hash_tags))
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
            if recv_json is sock_req.error: return (None, None)
            return recv_json['return_info']['k_list'][0]
        return (None, None)
        
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
        self.sock_rep.send_json({"response": "ACK", "procedence_address": self.address})
    
    
    def command_get_tag(self, tag, sock_req):
        tag_id = int(hashlib.sha1(bytes(tag, 'utf-8') ).hexdigest(),16)
        if self.address == self.predecessor_address or self.id == tag_id or self.between(tag_id, (self.predecessor_id, self.id)):
            tags_object_id = self.get_tag(tag_id)
            recv_json = {"response": "ACK", "tags_object_id" : tags_object_id}
        else:
            destination_id, new_destination_address = self.find_successor(tag_id, sock_req)
            recv_json = sock_req.make_request(json_to_send = {'command_name': 'get_tag', 
                                                                'method_params': {'tag': tag}, 
                                                                "procedence_address": self.address, 
                                                                "procedence_method": "command_get_tag"}, 
                                                requester_object= self, 
                                                destination_id = destination_id, 
                                                destination_address = new_destination_address )
        self.sock_rep.send_json(recv_json)

    def get_tag(self, tag_id):
        if tag_id in self.hash_tags:
            return self.hash_tags[tag_id]
        else:
            return {}

    
    def command_send_file(self, path, sock_req):
        print("command send file")
        
        object_id = int(hashlib.sha1(bytes(path, 'utf-8') ).hexdigest(),16)
        
        if self.address == self.predecessor_address or self.id == object_id or self.between(object_id, (self.predecessor_id, self.id)):
            recv_json =  {"response": "ACK", "return_info": {"address" : self.address } } 
        else:
            destination_id, new_destination_address = self.find_successor(object_id, sock_req)
            recv_json = sock_req.make_request(json_to_send = {'command_name': 'send_file', 
                                                        'method_params': {'path': path}, 
                                                        "procedence_address": self.address, 
                                                        "procedence_method": "command_send_file"}, 
                                        requester_object= self, 
                                        destination_id = destination_id, 
                                        destination_address = new_destination_address)
        self.sock_rep.send_json(recv_json)
        
    def send_file(self, path):
        
        if not os.path.isfile("data/" + str(self.id) + "/" + path):
            self.sock_rep.send(b'')
            return
        
        print("Existe el archivo")
        
        fn = open("data/" + str(self.id) + "/" + path, 'rb')
        stream = True
        
        print("Enviando data")
        
        while stream:
            stream = fn.read(128)
            if stream:
                self.sock_rep.send(stream, zmq.SNDMORE)
            else:
                self.sock_rep.send(stream)

    def send_tags_for_replication(self, list_tags):
        self.sock_rep.send_json({"response": "ACK", "return_info": {}})
        for tag in list_tags:
            if not int(tag) in self.replication["tags"]:
                self.replication["tags"][int(tag)] = {}
            for key in list_tags[tag]:
                self.replication["tags"][int(tag)][int(key)] = list_tags[tag][key]
        
    
    def get_tag_for_replication(self):
        self.sock_rep.send_json({"response": "ACK", "return_info": {"tags" : self.hash_tags}})
    
    def send_files_for_replication(self, sock_req):
        self.sock_rep.send_json({"response": "ACK", "return_info": {}})
        
        os.system("ls ./data/" + str(self.id) + " > data/" + str(self.id) + "/replication_to_send_temp.txt")
        replication_temp_file = open("data/" + str(self.id) + "/replication_to_send_temp.txt", 'r')
        list_of_files = replication_temp_file.read().split("\n")
        
        pos = []
        
        for i in range(len(list_of_files)):
            if (list_of_files[i] == "Replication" or
                list_of_files[i] == "" or
                list_of_files[i] == "replication_to_send_temp.txt" or
                list_of_files[i] == "replication_temp.txt"):
                
                pos.append(i)
                
        for i in range(len(pos) - 1, -1, -1):
            del(list_of_files[pos[i]])
            
        actual_successor = self.finger_table[0]
        
        recv_json = sock_req.make_request(json_to_send = {"command_name" : "get_files_for_replication", 
                                                "method_params" : {"list_files" : list_of_files,
                                                                    "destination_address" : self.address}, 
                                                "procedence_address" : self.address}, 
                                destination_id = actual_successor[0], 
                                destination_address = actual_successor[1])

        os.system("rm data/" + str(self.id) + "/replication_to_send_temp.txt")
    
    def get_files_for_replication(self, list_files, destination_address):
        self.sock_rep.send_json({})
        
        for path in list_files:
            
            if path == "":
                continue
            
            dest = open("data/" + str(self.id) + "/Replication/" + os.path.basename(path), 'wb')
            socket_request = self.context.socket(zmq.REQ)
        
            socket_request.connect('tcp://' + str(destination_address))
            print('tcp://' + destination_address)
            
            socket_request.send_json({"command_name": "get_object", 
                                           "method_params": {"path" : path}, 
                                           "procedence_addr": self.address})
                
            while True:
                data = socket_request.recv()
                dest.write(data)
                if not socket_request.getsockopt(zmq.RCVMORE):
                    break
            socket_request.disconnect("tcp://" + str(destination_address))
            socket_request.close()
    
    def cut_file(self, path, sock_req):
        if not os.path.isfile("data/" + str(self.id) + "/" + path):
            print("data/" + str(self.id) + "/" + path)
            self.sock_rep.send(b'')
            return
        
        print("Existe el archivo")
        
        fn = open("data/" + str(self.id) + "/" + path, 'rb')
        stream = True
        print("Enviando data")
        
        while stream:
            stream = fn.read(128)
            if stream:
                self.sock_rep.send(stream, zmq.SNDMORE)
            else:
                self.sock_rep.send(stream)
        
        #Ahora borrar el archivo
        if os.path.exists("data/" + str(self.id) + "/" + path):
            os.system("rm " + "data/" + str(self.id) + "/" + path)
            print('Archivo borrado con exito.')

            successor = self.finger_table[0]
            
            recv_json = sock_req.make_request(json_to_send = {'command_name': 'delete_object_from_replication', 
                                                              'method_params': {'path': path}},  
                                            destination_id = successor[0], 
                                            destination_address = successor[1])
            
            
    def delete_object_from_replication(self, path):
        self.sock_rep.send_json({})
        if os.path.exists("data/" + str(self.id) + "/Replication/" + path):
            os.system("rm " + "data/" + str(self.id) + "/Replication/" + path)
        
            

    def command_recv_file(self, path, destination_address, tags, sock_req):
        self.sock_rep.send_json({})   
        
        object_id = int(hashlib.sha1(bytes(path, 'utf-8') ).hexdigest(),16)
        
        if self.address == self.predecessor_address or self.id == object_id or self.between(object_id, (self.predecessor_id, self.id)):
        
            dest = open("data/" + str(self.id) + "/" + os.path.basename(path), 'wb')
            socket_request = self.context.socket(zmq.REQ)
        
            socket_request.connect('tcp://' + str(destination_address))
            print('tcp://' + destination_address)
            socket_request.send(path.encode())
            print("Yo: " + str(self.address) + str(self.id))
            print("Guardo objeto: " + str(path))

            while True:
                data = socket_request.recv()
                dest.write(data)
                if not socket_request.getsockopt(zmq.RCVMORE):
                    break
            socket_request.disconnect("tcp://" + str(destination_address))
            socket_request.close()
            
            tags = tags[1:len(tags)-1].split(",")
            for tag in tags:
                tag_id = int(hashlib.sha1(bytes(tag, 'utf-8') ).hexdigest(),16)
                if self.address == self.predecessor_address or self.id == object_id or self.between(tag_id, (self.predecessor_id, self.id)):
                    # Agregamos la etiqueta actual a las nuestras
                    self.recv_tag(path, object_id, tag_id, sock_req)
                else:
                    destination_id, new_destination_address = self.find_successor(tag_id, sock_req)
                    recv_json = sock_req.make_request(json_to_send = {'command_name': 'recv_tag', 
                                                                      'method_params': {'path': path,
                                                                                        'path_id': object_id, 
                                                                                        'tag_id': tag_id}, 
                                                                      "procedence_address": self.address, 
                                                                      "procedence_method": "recv_file"}, 
                                                      requester_object= self, 
                                                      destination_id = destination_id, 
                                                      destination_address = new_destination_address )
            
            actual_successor = self.finger_table[0]
            
            list_of_files = [path]
            
            recv_json = sock_req.make_request(json_to_send = {"command_name" : "get_files_for_replication", 
                                                                        "method_params" : {"list_files" : list_of_files,
                                                                                        "destination_address" : self.address}, 
                                                                        "procedence_address" : self.address}, 
                                                        destination_id = actual_successor[0], 
                                                        destination_address = actual_successor[1])
            
        else:
            destination_id, new_destination_address = self.find_successor(object_id, sock_req)
            recv_json = sock_req.make_request(json_to_send = {'command_name': 'recv_file', 
                                                                      'method_params': {'path': path, 
                                                                                        'destination_address': destination_address,
                                                                                        'tags': tags}, 
                                                                      "procedence_address": self.address, 
                                                                      "procedence_method": "recv_file"}, 
                                                      requester_object= self, 
                                                      destination_id = destination_id, 
                                                      destination_address = new_destination_address )

    def command_recv_tag(self, path, path_id, tag_id, sock_req):
        self.recv_tag(path, path_id, tag_id, sock_req)
        print("Objeto " + str(path_id) + " guardado en " + str(tag_id))
        self.sock_rep.send_json({"response": "ACK", "return_info": {}})

    def recv_tag(self, path, path_id, tag_id, sock_req):
        if tag_id in self.hash_tags:
            if path_id in self.hash_tags[tag_id]:
                return
            self.hash_tags[tag_id][path_id] = path
        else:
            self.hash_tags[tag_id] = {path_id : path}
        
        # Enviamos dicha etiqueta al sucesor para la replicación

        actual_successor = self.finger_table[0]

        list_of_tags = {tag_id : {path_id:path}}
        
        recv_json = sock_req.make_request(json_to_send = {"command_name" : "send_tags_for_replication", 
                                                                    "method_params" : {"list_tags" : list_of_tags}, 
                                                                    "procedence_address" : self.address}, 
                                                    destination_id = actual_successor[0], 
                                                    destination_address = actual_successor[1])

        
    def command_update_predecessor(self, predecessor_id, predecessor_address):
        print("entro comando get predecessor")
        self.predecessor_id = predecessor_id
        self.predecessor_address = predecessor_address
        self.sock_rep.send_json({"response": "ACK", "return_info": {}})

    def command_get_successor(self, x):
        print("entro comando get successor")
        id, address = self.finger_table[x-1][0],self.finger_table[x-1][1]
        self.sock_rep.send_json({"response": "ACK", "return_info": {"successor_pos_x_id": id, "successor_pos_x_address": address}})

