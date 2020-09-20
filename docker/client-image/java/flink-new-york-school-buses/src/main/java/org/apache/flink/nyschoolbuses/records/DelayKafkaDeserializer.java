package org.apache.flink.nyschoolbuses.records;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import java.io.IOException;
import java.nio.charset.StandardCharsets;


public class DelayKafkaDeserializer extends AbstractDeserializationSchema<Delay> {

    @Override
    public Delay deserialize(byte[] bytes) throws IOException {
        String delayInfoString = new String(bytes, StandardCharsets.UTF_8);

        String[] delayFields = delayInfoString.split(";");
        String occurredOn = delayFields[0];
        String boro = delayFields[1];
//        Double delay = Double.valueOf(delayFields[2]);
        String delay = delayFields[2];
        Delay output = new Delay(occurredOn, boro, delay);
        return output;
    }
}
