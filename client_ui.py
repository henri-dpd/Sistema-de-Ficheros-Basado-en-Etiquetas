from audioop import add
import os
import zmq

class client:
    def __init__(self, address):
        print("iniciado cliente en la direccion: " + str(address))
        self.address = address
        self.context = zmq.Context()
        self.sock_req = self.context.socket(zmq.REQ)
        self.sock_rep = self.context.socket(zmq.REP)
        self.sock_rep.bind("tcp://" + address) 
        

    def send_info(self, ip, command, params):
        
        self.sock_req.connect("tcp://"+ ip)
        
        print({"command_name": command, "method_params": params , "procedence_addr": self.address})                
        self.sock_req.send_json({"command_name": command, "method_params": params , "procedence_addr": self.address})
        if command == "recv_file":
            self.sock_req.recv_json()
            info = self.send_file(params["path"])
        else:
            info = self.sock_req.recv_json()
        print(info)
        self.sock_req.disconnect("tcp://"+ ip)
        
        if command == "send_file":
            print("get file")
            self.sock_req.connect("tcp://"+ info["return_info"]["address"])
            self.sock_req.send_json({"command_name": "get_object", "method_params": {"path" : params["path"]}, "procedence_addr": self.address})
            self.get_file(params["path"])
            self.sock_req.disconnect("tcp://"+ info["return_info"]["address"])
                
            
    def get_file(self, path):
        # Abrimos el fichero en el que vamos a escribir
        
        try:
            os.mkdir("recv_client_data")
        except:
            pass
        
        dest = open("recv_client_data/" + os.path.basename(path), 'wb')
        print("Recibiendo data")
        while True:
            
            # Comenzamos a recibir la data
            data = self.sock_req.recv()
            
            # Escribimos en el file
            dest.write(data)
            
            if not self.sock_req.getsockopt(zmq.RCVMORE):
                break
        print("recived") 
    
    def send_file(self, path):
        recv = self.sock_rep.recv()
        # Verificamos que el fichero es valido
        
        try:
            os.mkdir("client_data")
        except:
            pass
        
        if not os.path.isfile("client_data/" + path):
            self.sock_rep.send('')
            return
        
        print("Leyendo datos")
        
        # Abrimos el fichero a copiar
        fn = open("client_data/" + path, 'rb')
        stream = True
        
        # Comenzamos a leer en el fichero
        print("Enviando datos")
        
        while stream:
            # Leemos el fichero bit por bit
            stream = fn.read(128)
            if stream:
                # Si el stream debe mandar más datos
                self.sock_rep.send(stream, zmq.SNDMORE)
            else:
                # Terminamos de enviar, sin flag
                self.sock_rep.send(stream)   
        return "finished"

