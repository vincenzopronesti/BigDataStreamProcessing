#!/bin/bash

echo "Executing Query 2 ..."

echo "1/4 - Creating Kafka topics"
echo "    Creating input topic"
/script/bin/kafka-topics.sh --create --bootstrap-server kafka0:9092 --replication-factor 1 --partitions 1 --topic input-query-2
echo "    Creating output topic for 24h query"
/script/bin/kafka-topics.sh --create --bootstrap-server kafka0:9092 --replication-factor 1 --partitions 1 --topic output-query-2-day
echo "    Creating output topic for 1 week query"
/script/bin/kafka-topics.sh --create --bootstrap-server kafka0:9092 --replication-factor 1 --partitions 1 --topic output-query-2-week

echo "2/4 - Submitting topology to Flink JobManager ..."
flink run -d -p 3 -c org.apache.flink.nyschoolbuses.Query2 /opt/NYSchoolBusJob.jar --latency --bootstrap.servers kafka0:9092 --input-topic input-query-2 --output-topic-day output-query-2-day --output-topic-week output-query-2-week

echo "3/4 - Creating Kafka consumers for output topics ..."
touch query-2-day.csv
/script/bin/kafka-console-consumer.sh --bootstrap-server kafka0:9092 --topic output-query-2-day --from-beginning > /script/query-2-day.csv &

touch query-2-week.csv
/script/bin/kafka-console-consumer.sh --bootstrap-server kafka0:9092 --topic output-query-2-week --from-beginning > /script/query-2-week.csv &

echo "4/4 - Starting Kafka producer for input data ..."
java -classpath /opt/NYSchoolBusJob.jar org.apache.flink.nyschoolbuses.DataGenerator --bootstrap.servers kafka0:9092 --topic input-query-2