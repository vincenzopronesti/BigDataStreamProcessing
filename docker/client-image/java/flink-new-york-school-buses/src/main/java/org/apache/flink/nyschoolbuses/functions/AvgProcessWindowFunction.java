package org.apache.flink.nyschoolbuses.functions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class AvgProcessWindowFunction extends ProcessAllWindowFunction<Tuple2<String, Double>, String, TimeWindow> {
    private transient Meter avgThroughputMeter;
    private String operatorName;

    public AvgProcessWindowFunction(String operatorName) {
        super();
        this.operatorName = operatorName;
    }

    @Override
    public void open(Configuration config) {
        com.codahale.metrics.Meter dropWizardMeter = new com.codahale.metrics.Meter();
        this.avgThroughputMeter =
                getRuntimeContext()
                        .getMetricGroup()
                        .meter(operatorName + "AvgThroughput", new DropwizardMeterWrapper(dropWizardMeter));
    }

    @Override
    public void process(Context context, Iterable<Tuple2<String, Double>> iterable, Collector<String> collector) throws Exception {
        long startTimestamp = context.window().getStart();
        Date date = new Date(startTimestamp);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss", Locale.US);
        String printableStartTimestamp = sdf.format(date);
        StringBuilder output = new StringBuilder(printableStartTimestamp + "");
        for (Tuple2<String, Double> tuple : iterable) {
            output.append(", " + tuple.f0 + ", " + tuple.f1.toString());
        }
        collector.collect(output.toString());

        this.avgThroughputMeter.markEvent();
    }
}
