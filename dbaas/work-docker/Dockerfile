FROM python:3.7-slim-stretch
RUN pip install pika kazoo pymongo gevent
RUN apt update
RUN apt install -y procps
RUN apt install -y mongodb