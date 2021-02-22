FROM python:3.7-alpine

RUN mkdir -p /app
WORKDIR /app

RUN pip install kafka-python

COPY kafka_print.py /app

CMD ["python", "/app/kafka_print.py"]