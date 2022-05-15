
import rpyc

client = rpyc.connect("localhost", 8001)

client.root.getdocument("0.0.0.2", "8000", "8001")
