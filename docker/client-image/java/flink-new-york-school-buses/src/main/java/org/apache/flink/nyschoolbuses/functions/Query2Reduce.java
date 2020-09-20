package org.apache.flink.nyschoolbuses.functions;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class Query2Reduce implements ReduceFunction<Tuple2<Tuple2<String, String>, Integer>> {

    @Override
    public Tuple2<Tuple2<String, String>, Integer> reduce(Tuple2<Tuple2<String, String>, Integer> tuple2IntegerTuple2, Tuple2<Tuple2<String, String>, Integer> t1) throws Exception {
        return new Tuple2<>(tuple2IntegerTuple2.f0, tuple2IntegerTuple2.f1 + t1.f1);
    }
}