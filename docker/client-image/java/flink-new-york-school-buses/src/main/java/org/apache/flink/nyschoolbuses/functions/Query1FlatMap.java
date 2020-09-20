package org.apache.flink.nyschoolbuses.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.nyschoolbuses.records.Delay;
import org.apache.flink.util.Collector;

public class Query1FlatMap implements FlatMapFunction<Delay, Tuple2<String, Double>>{//, ResultTypeQueryable<Tuple2<String, Double>> {
    @Override
    public void flatMap(Delay delay, Collector<Tuple2<String, Double>> collector) throws Exception {
        collector.collect(new Tuple2<>(delay.getBoro(), delay.getDelay()));
    }
}
