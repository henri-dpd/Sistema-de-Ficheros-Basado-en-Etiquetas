import hashlib
from random import Random
import math
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
        self.id = 0
        self.context = zmq.Context()
        self.size = 64
        self.start = lambda i : (self.id + 2**(i)) % 2**self.size
        self.finger_table_length = int(math.log2(self.size))
        self.finger_table = []
        self.waiting_time = 10
        self.nodes_to_keep = 2
        self.predecessor_id = None
        self.predecessor_address = None
        self.isPrincipal = False
    
        self.commands = {"join": self.command_join, 
                         "it_lost_a_node": self.command_it_lost_a_node,
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
                         "get_in_to_chord_succefully" : self.get_in_to_chord_succefully
                         }

        self.commands_request = { "verify_alive_node", "it_lost_a_node", "calculate_id_in",
                                  "get_in_new_node", "replace_anteccessor", "replace_finger_table_consecutive",
                                  "get_in_to_chord_succefully", "join"}

        print("Started node ", (self.id, self.address))

        client_requester = request(context = self.context)
        if introduction_node:
            recieved_json = client_requester.make_request(json_to_send = {"command_name" : "join",
                                                                         "method_params" : {"node_address" : self.address}, 
                                                                         "procedence_addr" : self.address}, 
                                                          destination_address = introduction_node)
            
            while recieved_json is client_requester.error:
                client_requester.action_for_error(introduction_node)

                print("Connecting now to ", (introduction_node))
                
                recieved_json = client_requester.make_request(json_to_send = {"command_name" : "join", 
                                                                              "method_params" : {"node_address" : self.address}, 
                                                                              "procedence_addr" : self.address}, 
                                                              destination_address = introduction_node)
                        
        else:
            self.predeccesor_address, self.predeccesor_id = self.address, self.id
            
            self.isPrincipal = True

        self.execute(client_requester)


    def waiting_for_commands(self, client_request):
        
        print("Entro a waiting for commands")
        self.sock_rep = self.context.socket(zmq.REP)
        self.sock_rep.bind("tcp://" + self.address) 
                
        while True:

            print("Waiting")
            
            buffer = self.sock_rep.recv_json()

            if buffer['command_name'] in self.commands:
                
                print(buffer)
                if buffer['command_name'] in self.commands_request:
                    print("comando con request")
                    self.commands[buffer["command_name"]](**buffer["method_params"], sock_req = client_request)
                else:
                    print("comando sin request")
                    self.commands[buffer["command_name"]](**buffer["method_params"])



    def command_join(self, node_address, sock_req : request):
        self.sock_rep.send_json({"response": "ACK_to_join", "return_info": {}})
        self.calculate_id_in(node_address, self.id, None, None, 1, sock_req)

    def command_find_successor(self):
        print("entro comando find successor")
        id, address = self.finger_table[0][0],self.finger_table[0][1]
        self.sock_rep.send_json({"response": "ACK", "return_info": {"successor_id": id, "successor_address": address}})
        print("termino comando find successor")

    def command_find_predecessor(self):
        print("entro comando find predcessor")
        self.sock_rep.send_json({"response": "ACK", "return_info": {"predecessor_id": self.predecessor_id, "predecessor_address": self.predecessor_address}, "procedence_address": self.address } )
        print("salio comando find predecessor")

    def command_are_you_alive(self):
        print("entro comando are you alive")
        self.sock_rep.send_json({"response": "ACK", "procedence_addr": self.address})
        print("salio comando are you alive")

    def command_get_params(self):  
        print("entro comando get params")      
        self.sock_rep.send_json({"response": "ACK", "return_info": {"finger_table" : self.finger_table, "predecessor_id": self.predecessor_id, "predecessor_address": self.predecessor_address, "id": self.id, "address": self.address } })
        print("salio comando get params")

    def command_get_prop(self, prop_name):
        print("entro comando get prop")
        if prop_name == "start_indexes":
            self.sock_rep.send_json({'response': "ACK", "return_info" : [self.start(i) for i in range(self.size)] })    

        self.sock_rep.send_json({'response': 'ACK', "return_info": self.__dict__[prop_name] })
        print("salio comando get prop")

    def command_get_predecessor(self):
        print("entro comando get predecessor")
        self.sock_rep.send_json({"response": "ACK", "return_info": {"predecessor_id" : self.predecessor_id, "predecessor_address" : self.predecessor_address } } )
        
    def command_update_predecessor(self, predeccesor_id, predeccesor_address):
        self.predeccesor_id = predeccesor_id
        self.predecessor_address = predeccesor_address
        self.sock_rep.send_json({"response": "ACK", "return_info": {}})
        print("salio comando get predecessor")

    def command_get_successor(self, x):
        print("entro comando get successor")
        id, address = self.finger_table[x-1][0],self.finger_table[x-1][1]
        self.sock_rep.send_json({"response": "ACK", "return_info": {"successor_pos_x_id": id, "successor_pos_x_address": address}})
        print("salio comando get successor")

    def execute(self, client_requester):
        print("entro execute")
        thread_verify = threading.Thread(target = self.thread_verify, args =() )
        print("abrió hilo")
        thread_verify.start()
        print("termino thread verify")        
        self.waiting_for_commands(client_requester)

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

    def thread_verify (self):
        print("entro thread verify")
        countdown = time()
        rand = Random()
        rand.seed()
        requester = request(context = self.context)
        print("instancio request en thread verify")
        while True:
            if abs (countdown - time( ) ) > self.waiting_time:
                if self.predecessor_id != self.id:
                    self.verify_alive_nodes(sock_req = requester) 
                    print("paso command verify alive nodes")                    
                countdown = time()

             
    # Verificar si los nodos de la finger table siguen vivos
    def verify_alive_nodes(self, sock_req : request):                        
        if len(self.finger_table) == 0:
            return
        print("Entró a verify alive nodes")
        # Guardamos el address del nodo anterior al que se va de la red 
        # Este es quien se encarga de reajustar la red
        predecessor_address = self.address
        # Por cada nodo
        for finger_table_node in range(min(self.finger_table, self.nodes_to_keep)):
            print("Verificando si el nodo " + finger_table_node + " está vivo")
            # Verificar si el nodo esta vivo
            recv_json_alive = sock_req.make_request(
                json_to_send = {"command_name" : "are_you_alive", "method_params" : {}, "procedence_addr" : self.address, "procedence_method": "verify_alive_nodes"},  
                destination_address = self.finger_table[finger_table_node][1])
            # Si da error es porque nunca contestó y se asume que está muerto
            if recv_json_alive is sock_req.error:
                sock_req.action_for_error(self.finger_table[finger_table_node][1])
                print("Hemos perdido a " + finger_table_node + ", actuaremos en consecuencia")
                # Ejecutamos comando para estabilizar la red despues de perder al nodo
                sock_req.make_request(
                json_to_send = {"command_name" : "it_lost_a_node", "method_params" : {}, 
                                "procedence_addr" : self.address, "procedence_method": "verify_alive_nodes"},  
                destination_address = predecessor_address)
                continue
            # el predecesor ahora es el nodo de esta iteración quién acabamos de comprobar que esta vivo
            predecessor_address = self.finger_table[finger_table_node][1]
            
    
    
        
        
    # Verificar si los nodos de la finger table siguen vivos
    def command_it_lost_a_node(self, sock_req : request):
        
        print("Entró a it lost a node, eliminó el successor en la finger table")
        
        # Cantidad de nodos muertos al final de mi finger table
        losses_node = 0
        self.finger_table.pop(0)
        
        print("Actualiza la finger table de los log(n) anteriores")
        # Crea la finger table para tu predecesor
        new_finger_table = self.finger_table.copy()
        new_finger_table.insert(0, (self.id, self.address))
        # Arregla
        sock_req.make_request(
                json_to_send = {"command_name" : "replace_finger_table_consecutive", 
                                "method_params": {"finger_table": new_finger_table,
                                                  "iterations": len(self.finger_table)}, 
                                "procedence_addr": self.address, "procedence_method": "it_lost_a_node"}, 
                destination_address = self.predecessor_address)
        if recv_json_update_predecessor is sock_req.error:
                sock_req.action_for_error(self.predecessor_address)
        
        # Actualizar el predecesor de mi nuevo sucesor con mi id y direccion
        print("Actualiza el predecesor del sigiente nodo")
        recv_json_update_predecessor = sock_req.make_request(
            json_to_send = {"command_name" : "update_predecessor", "method_params" : {"predeccesor_id": self.id, "predeccesor_address": self.address}, 
                            "procedence_addr" : self.address, "procedence_method": "verify_alive_nodes"},  
            destination_address = self.finger_table[0][1])
        if recv_json_update_predecessor is sock_req.error:
                sock_req.action_for_error(self.finger_table[0][1])
        
        print("Pregunta por el ultimo nodo de la finger table")
        # Pedir ultimo nodo de la finger table
        recv_json_alive = sock_req.make_request(
            json_to_send = {"command_name" : "are_you_alive", "method_params" : {}, "procedence_addr" : self.address, "procedence_method": "verify_alive_nodes"},  
            destination_address = self.finger_table[len(self.finger_table-1)][1])
        
        # Mientras el ultimo nodo verificado este muerto... 
        while recv_json_alive is sock_req.error:
            sock_req.action_for_error(self.finger_table[len(self.finger_table-1-losses_node)][1])
            print("Algunos de los nodos del final de mi finger table están muertos")
            # Si todos los nodos de la finger table estan muertos... levanta las manos
            if len(self.finger_table-1-losses_node) < 0: 
                print("Todos los nodos de mi finger table estan muertos")
                return
            losses_node += 1
            # Preguntal si el nodo n-i esta vivo 
            recv_json_alive = sock_req.make_request(
                json_to_send = {"command_name" : "are_you_alive", "method_params" : {}, "procedence_addr" : self.address, "procedence_method": "verify_alive_nodes"},  
                destination_address = self.finger_table[len(self.finger_table-1-losses_node)][1]) 
        
        print("Preguntando por el sucesor del último nodo de la finger table para agregarlo")
        # Pregunta por el nodo a agregar a la finger table
        recv_json_successor = sock_req.make_request(
                json_to_send = {"command_name" : "get_successor", "method_params": {"x": losses_node}, "procedence_addr": self.address, "procedence_method": "it_lost_a_node"}, 
                destination_address = self.finger_table[len(self.finger_table-1-losses_node)][1])
        if recv_json_update_predecessor is sock_req.error:
                sock_req.action_for_error(self.finger_table[len(self.finger_table-1-losses_node)][1])
        
        print("Agrego el nodo a la finger table")
        #Agrega el nodo a tu finger table
        self.finger_table.append((recv_json_successor[0],recv_json_successor[1]))
    
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

    # Método para calcular el id que debe tener en el chord una pc que desea entrar
    def calculate_id_in(self, address_request, initial_id, best_id, best_address_to_in, best_score, sock_req : request):
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
                best_id = best_id / 2
            else:
                best_id = (best_id / 2) + 1
            self.get_in_new_node(address_request, best_id, sock_req)
            return 0
            

        # Primero calculamos el espaciamiento entre esta pc y la siguiente
        
        successor_id = self.finger_table[0][0]
        
        if self.id < self.successor_id:
            if self.successor_id - self.id > best_score:
                best_score = self.successor_id - self.id
                best_id = self.id + self.successor_id
                if best_id % 2 == 0:
                    best_id = best_id / 2
                else:
                    best_id = (best_id / 2) + 1
                best_address_to_in = self.address
        else:
            if self.size - self.id + self.successor_id > best_score:
                best_score = self.size - self.id + self.successor_id

                best_id = self.id + self.size + self.successor_id
                
                if best_id % 2 == 0:
                    best_id = best_id / 2
                else:
                    best_id = (best_id / 2) + 1

                if best_id > self.size:
                    best_id = best_id - self.size
                best_address_to_in = self.address

        # Luego revisamos todos los espaciamientos en la finger table

        for i in range(len(self.finger_table) - 1):

            first_id = self.finger_table[i][0]
            second_id = self.finger_table[i + 1][0]

            if self.finger_table[i][0] == initial_id:
                # Ya con esto recorrimos todo el chord y encontramos el mejor espaciamiento
                # Ahora le enviamos ese mensaje al nodo designado para que reciba al nuevo nodo
                recv_json = sock_req.make_request(json_to_send = {"command_name" : "get_in_new_node", 
                                                                  "method_params" : {"address_to_get_in": address_request,
                                                                                     "id_to_place": best_id}},
                                          requester_object = self,
                                          destination_address = best_address_to_in)
                return 0



            if  first_id < second_id:
                if first_id - second_id > best_score:
                    best_score = first_id - second_id
                    best_id = first_id + second_id
                    if best_id % 2 == 0:
                        best_id = best_id / 2
                    else:
                        best_id = (best_id / 2) + 1
                    best_address_to_in = self.finger_table[i][1]
            else:
                if self.size - first_id + second_id > best_score:
                    best_score = self.size - first_id + second_id

                    best_id = first_id + self.size + second_id
                    
                    if best_id % 2 == 0:
                        best_id = best_id / 2
                    else:
                        best_id = (best_id / 2) + 1

                    if best_id > self.number_nodes:
                        best_id = best_id - self.number_nodes
                    best_address_to_in = self.finger_table[i][1]

        # Enviar todos los datos actuales al nodo en la última posición de la finger table
        recv_json = sock_req.make_request(json_to_send = {"command_name" : "calculate_id_in", 
                                                                  "method_params" : {"address_request": address_request,
                                                                                     "initial_id" : initial_id, 
                                                                                     "best_id" : best_id, 
                                                                                     "best_address_to_in" : best_address_to_in,
                                                                                     "best_score" : best_score}},
                                          requester_object = self,
                                          destination_address = best_address_to_in)


    # Método para que un nodo entre a otro como su sucesor en el chord
    def get_in_new_node(self, address_to_get_in, id_to_place, sock_req : request):
        print("Ya es posible entrar al nuevo nodo")

        # Primero le damos nuestra finger table para que actualice la suya
        recv_json = sock_req.make_request(json_to_send = {"command_name" : "get_in_to_chord_succefully", 
                                                          "method_params" : {"new_id" : id_to_place,
                                                                             "finger_table" : self.finger_table,
                                                                             "predecessor_address" : self.address,
                                                                             "predecessor_id" : self.id}},
                                          requester_object = self,
                                          destination_address = address_to_get_in)
        
        

        if not recv_json is sock_req.error:
            #Luego actualizamos el antecesor del  sucesor del nuevo nodo
            recv_json = sock_req.make_request(json_to_send = {"command_name" : "replace_predeccessor", 
                                                              "method_params" : {"new_predecessor_address" : None,
                                                                                 "new_predecessor_id": None}},
                                             requester_object = self,
                                             destination_address = self.finger_table[0][1])

            if not recv_json is sock_req.error:
                # Luego modificamos la finger table del nodo actual
                if len(self.finger_table) == self.finger_table_length:
                    self.finger_table = self.finger_table[:len(self.finger_table) - 1]
                self.finger_table.insert(0, [id_to_place, address_to_get_in])

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

    def get_in_to_chord_succefully(self, new_id, finger_table, predecessor_address, predecessor_id, sock_req : request):
        print("Entré al chord satisfactoriamente")
        self.id = new_id
        self.finger_table = finger_table
        self.predeccesor_addr = predecessor_address
        self.predeccesor_id = predecessor_id


    def replace_predeccessor(self, new_predecessor_address, new_predecessor_id, sock_req : request):
        print("Voy a reemplazar a mi predecesor porque algo pasó")
        self.antecessor_id = new_predecessor_id
        self.predecessor_address = new_predecessor_address


    # Método para estabilizar las finger table de los nodos anteriores
    def replace_finger_table_consecutive(self, finger_table, iterations, sock_req: request):
        print("Voy a estabilizar mi finger table porque algo pasó")
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