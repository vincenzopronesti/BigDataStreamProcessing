package org.apache.flink.nyschoolbuses.functions;

import org.apache.flink.nyschoolbuses.records.Delay;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

public class Query1Timestamp extends AscendingTimestampExtractor<Delay> {

    @Override
    public long extractAscendingTimestamp(Delay delay) {
        return delay.buildTS();
    }
}