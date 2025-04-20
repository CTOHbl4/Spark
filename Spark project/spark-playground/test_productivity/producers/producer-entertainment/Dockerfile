FROM python:3

WORKDIR /app

COPY ./producers/producer.py /app/

COPY ./producers/producer-entertainment/ /app/

RUN pip install kafka-python six