package org.apache.flink.nyschoolbuses.functions;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

/*
Sum elements and finally produce the average value
 */
public class AvgAggregateFunction implements AggregateFunction<Tuple2<String, Double>, Tuple3<String, Double, Integer>, Tuple2<String, Double>> {

    @Override
    public Tuple3<String, Double, Integer> createAccumulator() {
        return new Tuple3<>("", 0d, 0);
    }

    @Override
    public Tuple3<String, Double, Integer> add(Tuple2<String, Double> stringDoubleTuple2, Tuple3<String, Double, Integer> stringDoubleIntegerTuple3) {
        return new Tuple3<>(stringDoubleTuple2.f0, stringDoubleTuple2.f1 + stringDoubleIntegerTuple3.f1, stringDoubleIntegerTuple3.f2 + 1);
    }

    @Override
    public Tuple2<String, Double> getResult(Tuple3<String, Double, Integer> stringDoubleIntegerTuple3) {
        return new Tuple2<>(stringDoubleIntegerTuple3.f0, stringDoubleIntegerTuple3.f1 / stringDoubleIntegerTuple3.f2);
    }

    @Override
    public Tuple3<String, Double, Integer> merge(Tuple3<String, Double, Integer> stringDoubleIntegerTuple3, Tuple3<String, Double, Integer> acc1) {
        return new Tuple3<>(stringDoubleIntegerTuple3.f0, stringDoubleIntegerTuple3.f1 + acc1.f1, stringDoubleIntegerTuple3.f2 + acc1.f2);
    }
}
