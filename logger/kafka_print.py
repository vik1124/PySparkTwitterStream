from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import time
import threading

KAFKA_BROKER = "kafka-broker:9092"
#KAFKA_BROKER = "localhost:9092"

def consume():
	consumer = KafkaConsumer(
		'test',
		bootstrap_servers=[KAFKA_BROKER],
		auto_offset_reset='latest',
		group_id='newone')
	print("Started Consumer")
	for message in consumer:
		print("%s\n" % (message.value.decode('utf-8')), flush=True)

def consumeAggDF():
	consumer = KafkaConsumer(
		'testout',
		bootstrap_servers=[KAFKA_BROKER],
		auto_offset_reset='earliest',
		group_id='newone')
	print("Started Consumer")
	for message in consumer:
		print("%s:%s" % (message.key.decode('utf-8'), message.value.decode('utf-8')), flush=True)

def safe_loop(fn):
	while True:
		try:
			fn()
		except SystemExit:
			print("Good bye!")
			return
		except NoBrokersAvailable:
			print("NoBrokersAvailable sleeping")
			time.sleep(2)
			continue
		except Exception as e:
			print(e)
			return

def main():
	print("Starting")
	consumer = threading.Thread(target=safe_loop, args=[consumeAggDF])
	consumer.start()
	consumer.join()


if __name__ == "__main__":
	main()