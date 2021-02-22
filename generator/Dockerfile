FROM python:3.7-alpine

RUN mkdir -p /app
WORKDIR /app

RUN pip install tweepy
RUN pip install kafka-python

COPY tweet_to_kafka.py /app
COPY creds.py /app

CMD ["python", "/app/tweet_to_kafka.py"]
