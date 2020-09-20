package org.apache.flink.nyschoolbuses.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.nyschoolbuses.records.DelayReason;
import org.apache.flink.util.Collector;

public class Query2FlatMapSlot implements FlatMapFunction<DelayReason, Tuple2<Tuple2<String, String>, Integer>>{//, ResultTypeQueryable {
    @Override
    public void flatMap(DelayReason delayReason, Collector<Tuple2<Tuple2<String, String>, Integer>> collector) throws Exception {
        Tuple2<String, String> tuple;
        if (delayReason.isSlotA())
            tuple = new Tuple2<>("Slot A", delayReason.getReason());
        else if (delayReason.isSlotB())
            tuple = new Tuple2<>("Slot B", delayReason.getReason());
        else
            return;
        collector.collect(new Tuple2<>(tuple, 1));
    }
}
