
import rpyc
import json
import zmq

class Chord(rpyc.Service):
    exposed_version = "1.0.0"
    
    def on_connect(self, conn):
        print(f"Connection from {conn}")
        return super().on_connect(conn)
    
    def on_disconnect(self, conn):
        print(f"Connection closed from {conn}")
        return super().on_disconnect(conn)

    def exposed_addit(self, name: str, ip: str):
        with open('hash_table.json', 'r+') as htf:
            hash_table = json.load(htf)
            hash_table[name] = ip
            htf.seek(0)
            json.dump(hash_table, htf, indent=4) 
            htf.truncate()
        return "Added " + name + " with ip: " + ip

    def exposed_whereis(self, name: str):
        with open('hash_table.json', 'r') as htf:
            hash_table = json.load(htf)
            ip = hash_table[name]
        return ip    
    
    """ 
    def exposed_getdocument(self, host: str, port1: str, port2: str):
        HOST = host
        PORT1 = port1
        PORT2 = port2

        context = zmq.Context()
        p1 = "tcp://"+ HOST +":"+ PORT1 # how and where to connect
        p2 = "tcp://"+ HOST +":"+ PORT2 # how and where to connect
        s = context.socket(zmq.REP) # create reply socket
        s.bind(p1) # bind socket to address
        s.bind(p2) # bind socket to address
        while True:
            message = s.recv_string() # wait for incoming message
            if not "STOP" in message: # if not to stop...
                s.send_string(message + "*") # append "*" to message
            else: # else...
                break # break out of loop and end
    """ 
    
    def exposed_doc(self):
        return """
            addit (self, name: string, ip string)
            whereis (self, name: string)
        """  
            #getdocument (self, host: str, port1: str, port2: str)


if __name__ == "__main__":
    from rpyc.utils.server import ThreadedServer
    server = ThreadedServer(Chord, hostname="0.0.0.0", port=8001)
    print("RPC server on 0.0.0.0:8001")
    server.start()
