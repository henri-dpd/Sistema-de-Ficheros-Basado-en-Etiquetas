import zmq 


class request:
    def __init__(self, context, debug_print, error = "ERROR",  request_timeout = 6e3, request_retries = 3):
        
        self.request_timeout = request_timeout
        self.request_retries = request_retries
        self.context = context
        self.error = error            
        self.socket_request = self.context.socket(zmq.REQ)
        self.debug_print = debug_print

    def make_request(self, json_to_send, destination_address, destination_id, requester_object = None, asked_properties = None, method_for_wrap = None, procedence = None):        
        if asked_properties and destination_address == json_to_send['procedence_address']:
            
            return {"response": "ACK", "procedence_address": json_to_send['procedence_address'], "return_info": {asked_property: requester_object.__dict__[asked_property] for asked_property in asked_properties } }
        if method_for_wrap and destination_address == json_to_send['procedence_address']:
                         
            return {"response": "ACK", "procedence_address": json_to_send['procedence_address'], "return_info": requester_object.__class__.__dict__ [method_for_wrap] (requester_object, **json_to_send['method_params'])}
        retried = False

        for i in range(self.request_retries, 0, -1):
                        
            self.socket_request.connect("tcp://" + destination_address)  
            print("Sending message %s to %s" %(json_to_send, destination_address))
            
            self.socket_request.send_json(json_to_send)            
            
            if self.socket_request.poll(self.request_timeout):
                

                recv = self.socket_request.recv_json()
                print("Recieved %s from %s" %(recv, destination_address))
                
                self.socket_request.disconnect("tcp://" + destination_address) 
                if retried: print("I did it %s, to %s " %(json_to_send, destination_address))
                return recv

            
            print("Retrying to connect, time: ", (self.request_retries - i) + 1)
                
            print("I'm trying to send %s to %s " %(json_to_send, destination_address))

            self.socket_request.disconnect("tcp://" + destination_address)
            retried = True
            self.socket_request.setsockopt(zmq.LINGER, 0)
            self.socket_request.close()
            
            self.socket_request = self.context.socket(zmq.REQ)
            

        return self.error
        
        
