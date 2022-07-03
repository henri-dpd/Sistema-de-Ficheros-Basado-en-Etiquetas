# Sistema-de-Ficheros-Basado-en-Etiquetas

---

Proyecto para la asignatura **Sistema Distribuidos**

## Integrantes

- Airelys Collazo Perez C-412
- Alejandro Escobar Giraudy C-412
- Henri Daniel Peña Dequero C-411

---

En este proyecto se quiere realizar un sistema de ficheros basados en etiquetas, para ello,  se debe desarrollar un sistema distribuido. Para el desarrollo de dicho sistema, primeramente se debe construir una tabla hash distribuida que controle la parte del servidor del sistema; también, se debe crear un cliente que sea capaz de interactuar con el usuario, usando una interfaz visual, y así responder a las necesidades del mismo, subir un nuevo fichero y buscar algún fichero a partir de una etiqueta y descargarlo. Durante el desarrollo de dicho sistema se debe tener en cuenta la tolerancia a fallas, pues aunque un servidor se caiga o se agregue uno nuevo, el cliente tiene que recibir una respuesta correcta.

---

Para la ejecución de la aplicación se necesita tener instalado docker para simular la red en donde estará el sistema distribuido.

Primeramente, se debe cargar la imagen de docker, para ello se debe ejecutar el siguiente comando:

``` C
docker build -t <name_image>:<tag>.
```

Luego, para abrir el primer servidor se debe ejecutar:

``` C
docker run --rm --name <name> -it -P -v $(pwd):/usr/src/app <name_image>:<tag>
```

Para abrir los próximos servidores se debe ejecutar el siguiente comando en otras terminales:

``` C
docker run --rm --name <name> -it -P -v $(pwd):/usr/src/app <name_image>:<tag> --addr_known "<ip>:<puerto>"
```

Luego, para abrir un cliente se ejecuta en otra terminal:

``` C
docker run --rm --name <name> -it -P -v $(pwd):/usr/src/app <name_image>:<tag> --client
```

Una vez ya se tenga un cliente abierto se pueden realizar las peticiones a este desde su interfaz.
