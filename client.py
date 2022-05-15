import rpyc

client = rpyc.connect("localhost", 8001)
print(client.root.addit("Juan", "1.1.1.1"))
print(client.root.addit("XMen", "1.2.2.2"))
print(client.root.whereis("Juan"))

