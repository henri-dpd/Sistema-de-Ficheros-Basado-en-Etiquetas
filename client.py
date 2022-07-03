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
        self.send_info()
        

    def send_info(self):
        while True:
            buffer = input().split()
            print(buffer)
            self.sock_req.connect("tcp://"+ buffer[0])
            
            params = {buffer[i] : buffer[i + 1] for i in range(2, len(buffer), 2) }     

            print({"command_name": buffer[1], "method_params": params , "procedence_addr": self.address})                
            self.sock_req.send_json({"command_name": buffer[1], "method_params": params , "procedence_addr": self.address})
            if buffer[1] == "recv_file":
                self.sock_req.recv_json()
                info = self.send_file(params["path"])
            else:
                info = self.sock_req.recv_json()
            print(info)
            self.sock_req.disconnect("tcp://"+ buffer[0])
            
            if buffer[1] == "send_file":
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
        
        # Crea la carpeta si  no existe
        try:
            os.mkdir("client_data")
        except:
            pass
        
        if not os.path.isfile("client_data/" + path):
            self.sock_rep.send('')
            return
        
        print("Leyendo datos")
        
        fn = open("client_data/" + path, 'rb')
        stream = True
        
        print("Enviando datos")
        
        while stream:
            # Leer 128 bits
            stream = fn.read(128)
            if stream:
                # Si no ha terminado de leer el archivo
                self.sock_rep.send(stream, zmq.SNDMORE)
            else:
                # Ultimos bits leídos del archivo
                self.sock_rep.send(stream)   
        return "finished"

