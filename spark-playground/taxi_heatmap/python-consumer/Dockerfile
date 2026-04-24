FROM python:3

WORKDIR /app

COPY ./python-consumer/consumer.py /app/

RUN pip install kafka-python six numpy

CMD [ "python", "./consumer.py" ]
