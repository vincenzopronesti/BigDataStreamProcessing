package org.apache.flink.nyschoolbuses.records;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class DelayReasonKafkaDeserializer extends AbstractDeserializationSchema<DelayReason> {
    @Override
    public DelayReason deserialize(byte[] bytes) throws IOException {
        String delayInfoString = new String(bytes, StandardCharsets.UTF_8);
        String[] delayFields = delayInfoString.split(";");
        String occurredOn = delayFields[0];
        String reason = delayFields[1];
        DelayReason output = new DelayReason(occurredOn, reason);
        return output;
    }
}
