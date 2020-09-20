package org.apache.flink.nyschoolbuses.util;

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class KafkaOutputSerializer implements KafkaSerializationSchema<String> {
    private String topic;

    public KafkaOutputSerializer(String topic) {
        super();
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(String inputElement, @Nullable Long l) {
        return new ProducerRecord<>(topic, inputElement.getBytes(StandardCharsets.UTF_8));
    }
}
