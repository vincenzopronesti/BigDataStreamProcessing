package org.apache.flink.nyschoolbuses.functions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.*;

public class ReasonProcessAllWindowFunction extends ProcessAllWindowFunction<Tuple2<Tuple2<String,String>, Integer>, String, TimeWindow> {
    private transient Meter avgThroughputMeter;
    private String operatorName;

    public ReasonProcessAllWindowFunction(String operatorName) {
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
    public void process(Context context, Iterable<Tuple2<Tuple2<String, String>, Integer>> iterable, Collector<String> collector) throws Exception {
        long startTimestamp = context.window().getStart();
        Date date = new Date(startTimestamp);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss", Locale.US);
        String printableStartTimestamp = sdf.format(date);
        StringBuilder output = new StringBuilder(printableStartTimestamp + ", ");

        List<Tuple2<String, Integer>> slotA = new ArrayList<>();
        List<Tuple2<String, Integer>> slotB = new ArrayList<>();

        // slot-reason-num
        for (Tuple2<Tuple2<String, String>, Integer> tuple : iterable) {
            if (tuple.f0.f0.equals("Slot A"))
                slotA.add(new Tuple2<>(tuple.f0.f1, tuple.f1));
            else if (tuple.f0.f0.equals("Slot B"))
                slotB.add(new Tuple2<>(tuple.f0.f1, tuple.f1));
        }
        slotA.sort(Comparator.comparing(item->item.f1));
        slotB.sort(Comparator.comparing(item->item.f1));

        output.append("Slot A");
        for (int i = 0; i < 3 && i < slotA.size(); i++) {
            output.append(", " + slotA.get(slotA.size() -i -1).f0);
        }
        output.append(", Slot B");
        for (int i = 0; i < 3 && i < slotB.size(); i++) {
            output.append(", " + slotB.get(slotB.size() -i -1).f0);
        }

        collector.collect(output.toString());

        this.avgThroughputMeter.markEvent();
    }


}
