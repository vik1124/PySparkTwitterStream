# PySparkTwitterStream
Create a new file called Creds.py under generator with your twitter API creds. 
Run the following
*sudo docker-compose build
*sudo docker-compose up
In a separate terminal window run
*sudo docker-compose logs -f consumer-worker
And presto see the updated running counts of all hashtags coming in from twitter

Uses twitter API to get tweets and then pipe them to a Kafka topic, spark then reads off the topic and uses the structured streaming library 
to compute the wordcount in realtime. It then pipes the data onto a second output kafka topic
The loggeris a simple KafkaConsumer that reads off the output topic and prints data on to the console.
