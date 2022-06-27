import argparse
from ast import arg
from multiprocessing.connection import Client
import re
from socket import gethostbyname, gethostname
from client import client
from server import Node
import subprocess
import os 

if __name__ == "__main__":     
    parser = argparse.ArgumentParser()
    parser.add_argument('--addr_ip', default = gethostbyname(gethostname()) + ":8080", 
                        help = "Esta es la direccion IP del nodo." +  
                                "Si no se da de entrada esta es escogida de forma automatica.")
    parser.add_argument('--addr_known', default = None, 
                        help = "Esta es la diereccion IP de un nodo en la red." + 
                        "Si unes este nodo a la red necesitas darle esta direccion, "+
                        "si no este nodo nunca sera annadido a la red.")
    parser.add_argument('--client', action = "store_true", default = None)
    
    matcher = re.compile("\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d{1,6}")
    args = parser.parse_args()
    error_message = "%s Debe ser introducido con este formato IP:port, este es la direccion con la que se va a comunicar el nodo, si quieres evitar errores esta debe ser unica. Esta es una mala entrada: %s"
    if not matcher.fullmatch(args.addr_ip.split()[0]) :
        parser.error(error_message %("addr_ip", args.addr_ip))
    if args.addr_known and not matcher.fullmatch(args.addr_known.split()[0]):
        parser.error(error_message %("addr_known", args.addr_known))
    if(args.client):
        process = subprocess.Popen(["streamlit", "run", 'streamlit_app.py', args.addr_ip])
        # client = client(address = args.addr_ip)
    else:
        node = Node(address = args.addr_ip, introduction_node = args.addr_known)
