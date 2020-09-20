package org.apache.flink.nyschoolbuses;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.nyschoolbuses.util.TimeReader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.*;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Properties;

public class DataGenerator {
    private final static String DATE_PATTERN = "yyyy-MM-dd'T'HH:mm:ss";
    private static final Integer EVENT_TIME_FIELD = 7;
    private static final Integer BORO_FIELD = 9;
    private static final Integer REASON_FIELD = 5;
    private static final Integer DELAY_FIELD = 11;
    private static final String TOPIC_INPUT_1 = "input-query-1";
    private static final String TOPIC_INPUT_2 = "input-query-2";
    private static final String INPUT_FILE = "/data/bus-breakdown-and-delays.csv";

    public static void parseDelayFromCSV(String topic, KafkaProducer<String, String> producer) throws FileNotFoundException {
        Reader in = new FileReader(INPUT_FILE);
        CSVParser records = null;
        try {
            records = CSVFormat.DEFAULT.newFormat(';').parse(in);
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (org.apache.commons.csv.CSVRecord record : records) {
            try {
                String occurredOn = record.get(EVENT_TIME_FIELD);
                if (occurredOn.equals("Occurred_On")) // skip first line
                    continue;
                String boro = record.get(BORO_FIELD).toUpperCase();

                String delayed = record.get(DELAY_FIELD);
//                Double delayed = TimeReader.parseDelay(record.get(DELAY_FIELD));

                // remove incomplete data
                if (occurredOn.equals(""))
                    continue;
                if (boro.equals(""))
                    continue;
                if (delayed.equals(""))
                    continue;
//                if (delayed == null)
//                    continue;

                // build timestamp
                long timestampSec = 0;
                try {
                    LocalDateTime occurredOnLocalDate = LocalDateTime.parse(occurredOn,
                            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS"));
                    ZonedDateTime zdt = occurredOnLocalDate.atZone(ZoneId.of("America/New_York"));
                    timestampSec = zdt.toInstant().toEpochMilli();
                } catch (DateTimeParseException e) {
                    continue;
                }

//                ProducerRecord<byte[], byte[]> tuple = new DelaySerializationSchema(topic).serialize(delay, null);

                String output = "" + occurredOn + ";" + boro + ";" + delayed.toString();
                ProducerRecord<String, String> tuple = new ProducerRecord<String, String>(topic, String.valueOf(timestampSec), output);

                producer.send(tuple);
            } catch (IllegalArgumentException e) {
                continue;
            }
        }
    }

    public static void parseReasonFromCSV(String topic, KafkaProducer<String, String> producer) throws FileNotFoundException {
        Reader in = new FileReader(INPUT_FILE);
        CSVParser records = null;
        try {
            records = CSVFormat.DEFAULT.newFormat(';').parse(in);
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (org.apache.commons.csv.CSVRecord record : records) {
            String occurredOn = record.get(EVENT_TIME_FIELD);
            if (occurredOn.equals("Occurred_On")) // skip first line
                continue;
            String reason = record.get(REASON_FIELD).toLowerCase();
            if (occurredOn.equals(""))
                continue;
            if (reason.equals(""))
                continue;

            // build timestamp
            long timestampSec = 0;
            try {
                LocalDateTime occurredOnLocalDate = LocalDateTime.parse(occurredOn,
                        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS"));
                ZonedDateTime zdt = occurredOnLocalDate.atZone(ZoneId.of("America/New_York"));
                timestampSec = zdt.toInstant().toEpochMilli();
            } catch (DateTimeParseException e) {
                continue;
            }

//            DelayReason delayReason = new DelayReason(occurredOn, reason);
//            ProducerRecord<byte[], byte[]> tuple = new DelayReasonSerializationSchema(topic).serialize(delayReason, null);

            String output = "" + occurredOn + ";" + reason;
            ProducerRecord<String, String> tuple = new ProducerRecord<String, String>(topic, String.valueOf(timestampSec), output);

            producer.send(tuple);
        }
    }

    public static void main(String[] args) {
        final ParameterTool params = ParameterTool.fromArgs(args);

        String topic = params.get("topic");

        Properties kafkaProps = createKafkaProperties(params);

//        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(kafkaProps);
        KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps);

        if (topic.equals(TOPIC_INPUT_1)) {
            try {
                parseDelayFromCSV(topic, producer);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        } else if (topic.equals(TOPIC_INPUT_2)) {
            try {
                parseReasonFromCSV(topic, producer);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
        producer.close();

    }

    private static Properties createKafkaProperties(final ParameterTool params) {
        String brokers = params.get("bootstrap.servers", "localhost:9092");
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);//// ByteArraySerializer.class.getCanonicalName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);//ByteArraySerializer.class.getCanonicalName());
        return kafkaProps;
    }
}
