from audioop import add
import os
import zmq

def singleton_dec(class_):
    instances = {}
    def getinstance(*args,**kwargs):
        if class_ not in instances:
            instances[class_] = class_(*args, **kwargs)
        return instances[class_]
    return getinstance
            
@singleton_dec  
class client:
    def __init__(self, address):
        print("iniciado cliente en la direccion: " + str(address))
        self.address = address
        self.context = zmq.Context()
        self.sock_req = self.context.socket(zmq.REQ)
        self.sock_rep = self.context.socket(zmq.REP)
        self.sock_rep.bind("tcp://" + address) 
        

    def send_info(self, address, command_name, params):
        self.sock_req.connect("tcp://"+ address)   

        print({"command_name": command_name, "method_params": params , "procedence_addr": self.address})                
        self.sock_req.send_json({"command_name": command_name, "method_params": params , "procedence_addr": self.address})
        if not self.sock_req.poll(2000):
            return "ERROR"    
        if command_name == "recv_file":
            self.sock_req.recv_json()
            info = self.send_file(params["path"])
        else:
            info = self.sock_req.recv_json()
        self.sock_req.disconnect("tcp://"+ address)
        
        #if command_name == "send_file":
            #print("get file")
            # self.sock_req.connect("tcp://"+ info["return_info"]["address"])
            # self.sock_req.send_json({"command_name": "get_object", "method_params": {"path" : params["path"]}, "procedence_addr": self.address})
            # info = self.get_file(params["path"])
            # self.sock_req.disconnect("tcp://"+ info["return_info"]["address"])
            
        if command_name == "get_parts_file":
            parts = info["return_info"]["parts"]
            for p in range(parts):
                new_params = params
                new_params["path"] = new_params["path"] + ".p" + str(p+1)
                self.sock_req.send_json({"command_name": "send_file", "method_params": new_params , "procedence_addr": self.address})
                info = self.sock_req.recv_json()
            
                
                self.sock_req.connect("tcp://"+ info["return_info"]["address"])
                self.sock_req.send_json({"command_name": "get_object", "method_params": {"path" : params["path"]}, "procedence_addr": self.address})
                info = self.get_file(params["path"])
                self.sock_req.disconnect("tcp://"+ info["return_info"]["address"])
                
        print(info)
            

    # 172.17.0.2:8080 send_file path file1.mf
    # 172.17.0.2:8080 send_file path file2.mf
    # 172.17.0.2:8080 send_file path file3.mf
    def get_file(self, path):
        # Open up the file we are going to write to
        
        dest = open("recv_client_data/" + os.path.basename(path), 'wb')
        print("Recibiendo data")
        while True:
            # Start grabing data
            data = self.sock_req.recv()
            # Write the chunk to the file
            dest.write(data)
            if not self.sock_req.getsockopt(zmq.RCVMORE):
                break
            
        dest.close()
        
        return "recived"
    
    # 172.17.0.2:8080 get_tag tag a
    # 172.17.0.2:8080 get_tag tag f
    # 172.17.0.2:8080 get_tag tag w
    # 172.17.0.2:8080 get_tag tag e
    # 172.17.0.2:8080 get_tag tag aa
    # 172.17.0.2:8080 get_tag tag ff
    #get_tag
    
    # 172.17.0.2:8080 recv_file path file1.mf destination_address 172.17.0.4:8080 tags [a,f,w]
    # 172.17.0.3:8080 recv_file path file2.mf destination_address 172.17.0.4:8080 tags [aa,f,w]
    # 172.17.0.2:8080 recv_file path file3.mf destination_address 172.17.0.4:8080 tags [aa,ff,w,e]
    def send_file(self, path):
        recv = self.sock_rep.recv()
        # Verify that the file is available
        if not os.path.isfile("client_data/" + path):
            self.sock_rep.send('')
            return
        print("Leyendo datos")
        # Open the file for reading
        fn = open("client_data/" + path, 'rb')
        stream = True
        # Start reading in the file
        print("Enviando datos")
        while stream:
            # Read the file bit by bit
            stream = fn.read(128)
            if stream:
                # If the stream has more to send then send more
                self.sock_rep.send(stream, zmq.SNDMORE)
            else:
                # Finish it off
                self.sock_rep.send(stream)   
        return "finished"
""" 
client()

 """