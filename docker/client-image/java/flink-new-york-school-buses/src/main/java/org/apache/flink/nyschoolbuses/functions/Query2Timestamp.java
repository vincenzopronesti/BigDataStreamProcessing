package org.apache.flink.nyschoolbuses.functions;

import org.apache.flink.nyschoolbuses.records.DelayReason;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

public class Query2Timestamp extends AscendingTimestampExtractor<DelayReason>{
    @Override
    public long extractAscendingTimestamp(DelayReason delayReason) {
        return delayReason.buildTS();
    }

//    @Override
//    public TypeInformation getProducedType() {
//        return Types.LONG;
//    }
}
