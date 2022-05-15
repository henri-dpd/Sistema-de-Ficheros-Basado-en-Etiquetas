import zmq


HOST = '0.0.0.2'
PORT1 = '8000'

context = zmq.Context()
p1 = "tcp://"+ HOST +":"+ PORT1 # how and where to connect
s = context.socket(zmq.REQ) # create request socket
s.connect(p1) # block until connected
s.send_string("Hello world 1") # send message
message = s.recv_string() # block until response
s.send_string("STOP") # tell server to stop
print(message) # print result