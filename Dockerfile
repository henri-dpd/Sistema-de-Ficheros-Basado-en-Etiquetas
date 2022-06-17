FROM python:3.7-slim

WORKDIR /usr/src/app

#Adding files.
COPY . .

#RUN Step
RUN pip install pyzmq


EXPOSE 8080

#Default running
ENTRYPOINT ["python", "diaz_chord.py"] 
