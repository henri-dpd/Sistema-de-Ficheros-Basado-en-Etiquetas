import zmq

class client:
    def __init__(self, ip_port):
        self.context = zmq.Context()
        self.sock_req = self.context.socket(zmq.REQ)
        self.send_info()
        

    def send_info(self):
        while True:
            buffer = input().split()
            self.sock_req.connect("tcp://"+ buffer[0])
            params = {buffer[i] : buffer[i + 1] for i in range(2, len(buffer), 2) }
            
            if "belong" in buffer:
                params['interval'] = params['interval'].split(',')
                params['interval'] = ( int ( params['interval'][0][1:]), int(params['interval'][1][:-1] ) )
                params['id'] = int(params['id'])
                
            if "find_successor" in buffer:
                params['id'] = int(params["id"])
                

            self.sock_req.send_json({"command_name": buffer[1], "method_params": params , "procedence_addr": "127.0.0.1:5050"})
            info = self.sock_req.recv_json()
            print(info)
            self.sock_req.disconnect("tcp://"+ buffer[0])