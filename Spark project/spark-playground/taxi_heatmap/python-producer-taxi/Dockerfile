FROM python:3

WORKDIR /app

COPY ./python-producer-taxi/ /app/

RUN pip install kafka-python six

CMD [ "python", "./producer.py" ]