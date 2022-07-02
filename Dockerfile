FROM python:3.7-slim

WORKDIR /usr/src/app

#Adding files.
COPY . .

#RUN Step
RUN pip install pyzmq
RUN pip install pyqt5


EXPOSE 8080

#Default running
ENTRYPOINT ["python", "main.py"] 
