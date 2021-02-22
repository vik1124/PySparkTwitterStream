from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

#KAFKA_BROKER = "localhost:9092"
KAFKA_BROKER = "kafka-broker:9092"

spark = SparkSession.builder.appName('tweets').getOrCreate()

lines = spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_BROKER).option("subscribe",'test').load()
lines.selectExpr("CAST(value AS STRING)")

words = lines.select(explode(split(lines.value, ' ')).alias('word'))
hashtags = words.filter(words['word'].startswith('#'))
wordCounts = hashtags.groupBy('word').count()
#wordCounts = wordCounts.orderBy(wordCounts['count'].desc())
# CONSOLE O/P
# query = wordCounts.writeStream.outputMode('complete').format('console').start()
# query.awaitTermination()

# KAFKA O/P
query = wordCounts.selectExpr("CAST(word AS STRING) as key","CAST(count AS STRING) as value") \
		.writeStream.format('kafka') \
		.outputMode('update') \
		.option("kafka.bootstrap.servers",KAFKA_BROKER) \
		.option("topic",'testout') \
		.option("checkpointLocation", "/home/vikram/Documents/sparktut/checkpoint") \
		.start()
query.awaitTermination()