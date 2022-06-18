import argparse
import re
from socket import gethostbyname, gethostname

from server import Node

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--addr_ip', default = gethostbyname(gethostname()) + ":8080", 
                        help = "Esta es la dirección IP del nodo." +  
                                "Si no se da de entrada esta es escogida de forma automática.")
    parser.add_argument('--addr_known', default = None, 
                        help = "Esta es la dierección IP de un nodo en la red." + 
                        "Si unes este nodo a la red necesitas darle esta dirección, "+
                        "si no este nodo nunca será añadido a la red.")
    
    parser.add_argument('--debug_print', action = "store_true", 
                        help = "Esta opción permite imprimir todas las acciones del nodo.")
    
    matcher = re.compile("\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d{1,6}")
    args = parser.parse_args()
    error_message = "%s Debe ser introducido con este formato IP:port, " 
    + "este es la dirección con la que se va a comunicar el nodo, " 
    + "si queires evitar errores esta debe ser única. Esta es una mala entrada: %s"
    if not matcher.fullmatch(args.addr_ip.split()[0]) :
        parser.error(error_message %("addr_ip", args.addr_ip))
    if args.addr_known and not matcher.fullmatch(args.addr_known.split()[0]):
        parser.error(error_message %("addr_known", args.addr_known))


    node = Node(addr = args.addr_ip, introduction_node = args.addr_known, debug_print = args.debug_print)
    