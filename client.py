import os
import threading
import zmq

class client:
    def __init__(self):
        self.context = zmq.Context()
        self.sock_req = self.context.socket(zmq.REQ)
        self.sock_rep = self.context.socket(zmq.REP)
        self.sock_rep.bind("tcp://" + "127.0.0.1:5050") 
        self.send_info()
        

    def send_info(self):
        while True:
            buffer = input().split()
            self.sock_req.connect("tcp://"+ buffer[0])
            
            params = {buffer[i] : buffer[i + 1] for i in range(2, len(buffer), 2) }     

                
            self.sock_req.send_json({"command_name": buffer[1], "method_params": params , "procedence_addr": "127.0.0.1:5050"})
            if buffer[1] == "send_file":
                info = self.get_file(params["path"])
            if buffer[1] == "recv_file":
                info = self.sock_req.recv_json()
                info = self.send_file(params["path"])
            else:
                info = self.sock_req.recv_json()
            print(info)
            
            self.sock_req.disconnect("tcp://"+ buffer[0])

    def get_file(self, path):
        # Open up the file we are going to write to
        dest = open("client_data/" + os.path.basename(path), 'wb')
        print("Recibiendo data")
        while True:
            # Start grabing data
            data = self.sock_req.recv()
            # Write the chunk to the file
            dest.write(data)
            if not self.sock_req.getsockopt(zmq.RCVMORE):
                # If there is not more data to send, then break
                break
        return "recived"     
    
    def send_file(self, path):
        print("Hola?")
        recv = self.sock_rep.recv()
        print(recv)
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

client()

