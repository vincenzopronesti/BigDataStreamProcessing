package org.apache.flink.nyschoolbuses;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.nyschoolbuses.functions.AvgAggregateFunction;
import org.apache.flink.nyschoolbuses.functions.AvgProcessWindowFunction;
import org.apache.flink.nyschoolbuses.functions.Query1FlatMap;
import org.apache.flink.nyschoolbuses.functions.Query1Timestamp;
import org.apache.flink.nyschoolbuses.records.Delay;
import org.apache.flink.nyschoolbuses.records.DelayKafkaDeserializer;
import org.apache.flink.nyschoolbuses.util.WindowSizes;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Query1 {

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //disabling Operator chaining to make it easier to follow the Job in the WebUI
        //        env.disableOperatorChaining();
        /* Flink comes with a feature called Latency Tracking. When enabled, Flink will insert
        so-called latency markers periodically at all sources. For each sub-task, a latency
        distribution from each source to this operator will be reported. The granularity of
        these histograms can be further controlled by setting metrics.latency.granularity as desired.

        Due to the potentially high number of histograms (in particular for metrics.latency.granularity: subtask),
        enabling latency tracking can significantly impact the performance of the cluster. It is recommended
        to only enable it to locate sources of latency during debugging.
        */
        if (params.has("latency"))
            env.getConfig().setLatencyTrackingInterval(5);

        String inputTopic = params.get("input-topic", "input-query-1" );
        String outputTopicDay = params.get("output-topic-day", "output-query-1-day");
        String outputTopicWeek = params.get("output-topic-week", "output-query-1-week");
        String outputTopicMonth = params.get("output-topic-month", "output-query-1-month");
        String brokers = params.get("bootstrap.servers", "localhost:9092");
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "query-1");

        List<Time> windowSizes = WindowSizes.getWindowSizesQuery1();
        List<String> outputTopics = new ArrayList<>();
        outputTopics.add(outputTopicDay);
        outputTopics.add(outputTopicWeek);
        outputTopics.add(outputTopicMonth);
        List<FlinkKafkaProducer<String>> kafkaProducers = getKafkaProducers(kafkaProps, outputTopics);

        FlinkKafkaConsumer<Delay> consumer = new FlinkKafkaConsumer<>(inputTopic, new DelayKafkaDeserializer(), kafkaProps);
        consumer.assignTimestampsAndWatermarks(new Query1Timestamp());

        DataStream<Delay> delays = env.addSource(consumer);

        // boro-delay
        KeyedStream<Tuple2<String, Double>, String> keyedDelays = delays
                .flatMap(new Query1FlatMap())
                .keyBy(item -> item.f0); // boro

        for (int i = 0; i < outputTopics.size(); i++) {
            /*
            The process method of the ProcessWindowFunction will be passed an iterator that
            contains only the pre-aggregated result, and a Context that provides access to both
            global and per-window state.
             */
            keyedDelays.timeWindow(windowSizes.get(i))
                    .aggregate(new AvgAggregateFunction())
                    .timeWindowAll(windowSizes.get(i))
                    .process(new AvgProcessWindowFunction(outputTopics.get(i)))
                    .addSink(kafkaProducers.get(i))
                    .name(outputTopics.get(i) + windowSizes.get(i).toString());

        }
		env.execute("Query 1");
    }

    private static List<FlinkKafkaProducer<String>> getKafkaProducers(Properties kafkaProps, List<String> outputTopics) {
        List<FlinkKafkaProducer<String>> res = new ArrayList<>();
        for (int i = 0; i < outputTopics.size(); i++) {
            FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<String>(outputTopics.get(i),
                    new KafkaOutputSerializer1(outputTopics.get(i)), kafkaProps, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);
            res.add(producer);
        }
        return res;
    }

    private static class KafkaOutputSerializer1 implements KafkaSerializationSchema<String> {
        private String topic;

        public KafkaOutputSerializer1(String topic) {
            super();
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(String inputElement, @Nullable Long l) {
            return new ProducerRecord<>(topic, inputElement.getBytes(StandardCharsets.UTF_8));
        }
    }
}
