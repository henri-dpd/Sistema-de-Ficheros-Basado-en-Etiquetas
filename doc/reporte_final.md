# Documentación

## Introducción

En este proyecto se quiere realizar un sistema de ficheros basados en etiquetas, para ello,  se debe desarrollar un sistema distribuido. Para el desarrollo de dicho sistema, primeramente se debe construir una tabla hash distribuida que controle la parte del servidor del sistema; también, se debe crear un cliente que sea capaz de interactuar con el usuario, usando una interfaz visual, y así responder a las necesidades del mismo, subir un nuevo fichero y buscar algún fichero a partir de una etiqueta y descargarlo. Durante el desarrollo de dicho sistema se debe tener en cuenta la tolerancia a fallas, pues aunque un servidor se caiga o se agregue uno nuevo, el cliente tiene que recibir una respuesta correcta.

## Lógica del sistema distribuido

El sistema distribuido consta de uno o varios servidores conectados de forma circular, todo esto transparente al cliente que solo necesita el ip de uno de los servidores de la red para enviar instrucciones. Esta red de servidores se comunican entre sí peer_to_peer enviando mensajes por sockets(REQ y REP) haciendo uso de ZMQ, cada uno está identificado por un id que dice en que lugar de la red ubicarse en la red y de que información debe hacerse responsable. Luego, cada nodo además de in id y un ip tiene un predecesor y una "finger_table" que se encarga de guardar los id de los nodos succesor((act_id + 2^i) % 2^len(finger_table)) con los que se puede hacer un búsqueda logarítmica en la cantidad de nodos para encontrar uno dado que si el nodo deseado no se encuentra en la finger table el peor caso es que sea mayor que el último lugar de la fingr table y tengamos que pedirle a este nodo que lo busque, este último lugar de la finger table queda por definición en la otra mitad de la red por lo que descartamos al menos la mitad de los nodos de la red. La búsqueda consiste en buscar el nodo predecesror más cercano que se tenga guardado en la finger_table, luego a este pedirle lo mismo hasta encontrar el predecesor del id buscado, a este le pedimos una lista de k sucesores, el primer elemento es el que devolvemos. Para mantener a la red debemos cada cierto tiempo se debe verificar si los nodos mantienen la integriadad de la red, cada nodo debe preguntar si el predecesir está vivo para arreglar en caso es que se pierde el predecesor, también se debe preguntar por el predecesor de mi sucesor y si es diferente a mi pues entró un nodo a la red por lo tanto hay que arreglar. Adicionalmente la finger table se mantiene de forma similar, cada cierto tiempo se actualiza un lugar de la misma usando el método find_successor. Con lo id se crea una tabla hash distribuida donde cada nodo se encarga de guardar y mantener objetos con hash menor o igual que su id y mayor que el id de su predecesor. En este caso es un sistema de ficheros basado en etiquetas, cada fichero se le halla un hash al nombre y se guarda en el nodo correspondiente, con la etiquetas pasa algo similar pero se guarda en un diccionario que tiene el nodo teniendo como llave el hash de la etiqueta y como llave otro diccionario que tiene cada fichero con esa etiqueta con llave el hash del nombre del fichero y como valor el nombre del mismo. De esta forma sabemos donde se encuentra cada fichero y/o etiqueta, para buscar un fichero basta con buscar el sucesor con el find_successor y pedirle a ese nodo que lo guarde o lo envíe, para saber cada fichero por etiqueta se busca con el find_successor y se devuelve el diccionario. Para la tolerancia a fallas se mantiene una lista de k sucesores en la que nos apoyamos para cuando se cae uno o varios nodos tener k sucesores a los que conectarnos y mantener la integridad de la red(Esto también se pudo hacer con la finger table pero es mucho más lento dado que habría que esperar a que estabilizara la red). Adicionalmente se hace réplicas a los ficheros y etiquetas guardando en el predecesor toda la información correspondiente al nodo. Para mantener las réplicas se tiene un hilo que verifica si las réplicas están correctas, si no están correctas significa que entró un nodo a la red o se fue un nodo. Si un nodo entró a la red como mi predecesor pues se borra las réplicas que tenía hasta el momento y replica lo que tiene el que entró. Si un nodo abandona la red el sucesor del mismo toma las réplicas y lo toma como suyo y le pide al nuevo predecesor que le replique la información. Por último cada vez que se introduce un fichero se hace la réplica del mismo y sus etiquetas correspondinetes.

## Definición de clases

El proyecto consta de las clases:
- client
- node
- request

### Cliente
La función de la clase client es comunicarse mediante un ip dado con la red de servidores, con el mismo propósito esta clase tiene un método send info que se encarga de esperar una instrucción y enviar un request a la red de servidores, en este caso los principales comando usado son: are_you_alive, send_file, recv_file, get_tags. Adicionalemente, para los casos de enviar y recibir información el client tiene los métodos send_info y recv_info dedicados específicamente a leer bit a bit y enviar por socket response, y a escribir bit a bit lo recibido por un socket request respectivamente.

### Servidor
La clase node define precisamente uno de los nodos(computadoras) de la red, la que es capaz de iniciar solo(como único nodo) o conectarse a una red ya existente mediante un ip dado en el constructor de la clase. Este tiene una dirección ip definida, un id calculado con el hash de la dirección ip, un diccionario de comandos usado para verificar cada comando que le llegue por soquet y ejecutar el método correspondiente. Este al ser instanciado lo primero que hace es crear una serie de atributos necesarios para el manteniemiento de la red como lo ser un nodo predecesor, una finger_table, un k_list, un diccionario de etiquetas y las replicadas además de un context de ZMQ, luego si el no nodo recibe un id para unirse a la red entonces es el primer nodo(y en este punto es la red de servidores en sí) así que empieza con todo por defecto, sino pide unirse y actualiza todos los atributos incluyendo las réplicas tanto de los tags como de los archivos que debería tener el nodo y los replicados de otros nodos, al terminar de unirse a la red el noto ejecuta el método execute. El método execute se encarga de abrir un hilo para verificar la red, estabilizar y verificar las réplicas, además empieza a esperar comandos por un socket response el que en algún momento recibe una peticón de ejecutar un comando mediante un json, usando este json indexado en el diccionario de comandos se ejecuta el comando que dice la petición. El hilo abierto en el execute se encarga de cada intervalos de tiempos definidos: arreglar la finger table(escogiendo un lugar de la misma y asignándole el valor correcto), ejecutar el método verify y rect encargados de corregir alguna anomalía en la red por la introducción o eliminación de un nodo, verificar si las réplicas estás correctas tras insercciones o eliminaciones de nodos. Entre los principales comandos que ejecuta se encuentran: 
- join: Petición para unirse a la red
- are_you_alive: Comprobar que el nodo esté vivo
- find_successor: Encontrar el nodo sucesor a un id dado
- recv_file: Recibir un archivo 
- send_file: Enviar un archivo
- rect: Verificar que el predecesor está vivo
- stabilize: Verificar que el predecesor de mi sucesor soy yo sino tengo un nuevo sucesor
- recv_tag: Guarda etiquetas en el diccionario del nodo
- get_tag: Devuelve los nombres e id de los archivos que pertenecen a una etiqueta
- get_object: Lee y envia un archivo bit a bit
- cut_object: Lee y envia un archivo bit a bit y luego lo elimina, usado para replicar
- send_files_and_tag_for_new_node: devuelve la lista de archivos y etiquetas que debe obtener un nodo que entra a la red
- get_files_for_replication: Pide al predecesor todos los archivos que debe guardar como réplica
- send_files_for_replication: Manda a pedir todos los archivos que deben replicar en el nodo sucesor
- get_tag_for_replication: Devuelve las etiquetas a replicar
- send_tags_for_replication: Guarda las etiquetas replicadas.

### Request

La clase request se encarga de encapsular el proceso de hacer un request para no tener que hacer el mismo proceso cada vez que el sistema necesite enviar un request, también verifica que este se devuelva en tiempo y se envia k veces para evitar errores debido a que haya surgido algún problema mientras se enviaba el mismo. El método encargado de hacer las verificaciones antes mencionadas es make_request. Además se tiene el método action_for_error que lo que hace es indicar que hubo un error en el request o algún nodo se cayó.


### Interfaz de usuario

La intefaz de usuario se desarrolló haciendo uso de la biblioteca de python PyQt5. Esta consta de dos páginas; la primera es para que el usuario una vez conectado pueda a partir de una etiqueta que él ingrese ver los ficheros que contienen a dicha etiqueta y descargar alguno si lo desea; la segunda página se encarga de que el cliente pueda subir ficheros de su pc al sistema.

## Ejecución de la aplicación

Para la ejecución de la aplicación se necesita tener instalado docker para simular la red en donde estará el sistema distribuido.

Primeramente, se debe cargar la imagen de docker, para ello se debe ejecutar el siguiente comando:

```
docker build -t <name_image>:<tag>.
```

Luego, para abrir el primer servidor se debe ejecutar:

```
docker run --rm --name <name> -it -P -v $(pwd):/usr/src/app <name_image>:<tag>
```
Para abrir los próximos servidores se debe ejecutar el siguiente comando en otras terminales:

```
docker run --rm --name <name> -it -P -v $(pwd):/usr/src/app <name_image>:<tag> --addr_known "<ip>:<puerto>"
```
Luego, para abrir un cliente se ejecuta en otra terminal:

```
docker run --rm --name <name> -it -P -v $(pwd):/usr/src/app <name_image>:<tag> --client
```
Una vez ya se tenga un cliente abierto se pueden realizar las peticiones a este desde su interfaz.