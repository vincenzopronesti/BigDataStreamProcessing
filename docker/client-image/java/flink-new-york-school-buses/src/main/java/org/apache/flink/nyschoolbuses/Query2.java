package org.apache.flink.nyschoolbuses;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.nyschoolbuses.functions.Query2FlatMapSlot;
import org.apache.flink.nyschoolbuses.functions.Query2Timestamp;
import org.apache.flink.nyschoolbuses.functions.ReasonProcessAllWindowFunction;
import org.apache.flink.nyschoolbuses.records.DelayReasonKafkaDeserializer;
import org.apache.flink.nyschoolbuses.util.WindowSizes;
import org.apache.flink.nyschoolbuses.records.DelayReason;
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
import java.util.*;

public class Query2 {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        if (params.has("latency"))
            env.getConfig().setLatencyTrackingInterval(5);

        String inputTopic = params.get("input-topic", "input-query-2");
        String outputTopicDay = params.get("output-topic-day", "output-query-2-day");
        String outputTopicWeek = params.get("output-topic-week", "output-query-2-week");
        String brokers = params.get("bootstrap.servers", "localhost:9092");
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "query-2");

        List<Time> windowSizes = WindowSizes.getWindowSizesQuery2();
        List<String> outputTopics = new ArrayList<>();
        outputTopics.add(outputTopicDay);
        outputTopics.add(outputTopicWeek);
        List<FlinkKafkaProducer<String>> kafkaProducers = getKafkaProducers(kafkaProps, outputTopics);


        FlinkKafkaConsumer<DelayReason> consumer = new FlinkKafkaConsumer<>(inputTopic, new DelayReasonKafkaDeserializer(), kafkaProps);
        DataStream<DelayReason> delayReason = env.addSource(consumer)
                .assignTimestampsAndWatermarks(new Query2Timestamp());

        // slot-reason-1
        KeyedStream<Tuple2<Tuple2<String, String>, Integer>, Tuple> slotReasonCount = delayReason
                .flatMap(new Query2FlatMapSlot())
                .keyBy(0);

        for (int i = 0; i < outputTopics.size(); i++) {
            slotReasonCount.timeWindow(windowSizes.get(i))
//                    .reduce(new Query2Reduce())
                    .reduce(new ReduceFunction<Tuple2<Tuple2<String, String>, Integer>>() {
                        @Override
                        public Tuple2<Tuple2<String, String>, Integer> reduce(Tuple2<Tuple2<String, String>, Integer> tuple2IntegerTuple2, Tuple2<Tuple2<String, String>, Integer> t1) throws Exception {
                            return new Tuple2<>(tuple2IntegerTuple2.f0, tuple2IntegerTuple2.f1 + t1.f1);
                        }
                    })
                    .timeWindowAll(windowSizes.get(i))
                    .process(new ReasonProcessAllWindowFunction(outputTopics.get(i)))
                    .addSink(kafkaProducers.get(i))
                    .name(outputTopics.get(i) + windowSizes.get(i).toString());
        }
        env.execute("Query 2");
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
