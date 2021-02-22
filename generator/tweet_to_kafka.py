from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json
import time
from creds import *

KAFKA_BROKER = "kafka-broker:9092"
#KAFKA_BROKER = "localhost:9092"

class TweetsListener(StreamListener):

	def __init__(self):
		self.safe_loop("test", True)

	def on_data(self, data):
		try:
			msg = json.loads( data )
			print("msg {}".format(msg['text']))
			self.safe_loop(msg, False)
			return True
		except BaseException as e:
			print("Error on_data: %s" % str(e))
		return True

	def on_error(self, status):
		print(status)
		return True

	def safe_loop(self, msg, flag = False):
		while True:
			try:
				if flag == True:
					self.producer = KafkaProducer(bootstrap_servers=[KAFKA_BROKER])
				else:
					self.producer.send('test', key=msg['created_at'].encode('utf-8'), value=msg['text'].encode('utf-8'))
					#self.producer.flush() #Slows down producer considerably
					#print("****DONE*****")
				return
			except SystemExit:
				print("Adios!")
				return
			except NoBrokersAvailable:
				print("sleeping")
				time.sleep(2)
				continue
			except Exception as e:
				print(e)
				return

def main():
	auth = OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token, access_secret)

	twitter_stream = Stream(auth, TweetsListener())
	twitter_stream.filter(track=['covid'])

if __name__ == "__main__":
	main()