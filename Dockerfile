FROM python:3.7-slim

WORKDIR /usr/src/app

COPY . .


RUN pip install pyzmq
RUN pip install pyqt5


EXPOSE 8080

# Comando que corre por defecto
ENTRYPOINT ["python", "main.py"] 
