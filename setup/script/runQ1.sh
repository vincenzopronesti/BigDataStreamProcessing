#!/bin/bash

echo "Executing Query 1 ..."

echo "1/4 - Creating Kafka topics"
echo "    Creating input topic"
/script/bin/kafka-topics.sh --create --bootstrap-server kafka0:9092 --replication-factor 1 --partitions 1 --topic input-query-1
echo "    Creating output topic for 24h query"
/script/bin/kafka-topics.sh --create --bootstrap-server kafka0:9092 --replication-factor 1 --partitions 1 --topic output-query-1-day
echo "    Creating output topic for 1 week query"
/script/bin/kafka-topics.sh --create --bootstrap-server kafka0:9092 --replication-factor 1 --partitions 1 --topic output-query-1-week
echo "    Creating output topic for 1 month query"
/script/bin/kafka-topics.sh --create --bootstrap-server kafka0:9092 --replication-factor 1 --partitions 1 --topic output-query-1-month

echo "2/4 - Submitting topology to Flink JobManager ..."
flink run -d -p 3 -c org.apache.flink.nyschoolbuses.Query1 /opt/NYSchoolBusJob.jar --latency --bootstrap.servers kafka0:9092 --input-topic input-query-1 --output-topic-day output-query-1-day --output-topic-week output-query-1-week --output-topic-month output-query-1-month
#flink run -d -p 2 -c org.apache.flink.nyschoolbuses.Query1 /opt/NYSchoolBusJob.jar --latency --bootstrap.servers kafka0:9092 --input-topic input-query-1 --output-topic-day output-query-1-day --output-topic-week output-query-1-week --output-topic-month output-query-1-month

echo "3/4 - Creating Kafka consumers for output topics ..."
touch query-1-day.csv
/script/bin/kafka-console-consumer.sh --bootstrap-server kafka0:9092 --topic output-query-1-day --from-beginning > /script/query-1-day.csv &

touch query-1-week.csv
/script/bin/kafka-console-consumer.sh --bootstrap-server kafka0:9092 --topic output-query-1-week --from-beginning > /script/query-1-week.csv &

touch query-1-month.csv
/script/bin/kafka-console-consumer.sh --bootstrap-server kafka0:9092 --topic output-query-1-month --from-beginning > /script/query-1-month.csv &

echo "4/4 - Starting Kafka producer for input data ..."
java -classpath /opt/NYSchoolBusJob.jar org.apache.flink.nyschoolbuses.DataGenerator --bootstrap.servers kafka0:9092 --topic input-query-1	