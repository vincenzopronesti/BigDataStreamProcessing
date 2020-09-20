package org.apache.flink.nyschoolbuses.util;

import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;

public class WindowSizes {
    public static List<Time> getWindowSizesQuery1() {
        List<Time> windowSizes = new ArrayList<>();
        windowSizes.add(Time.hours(24));
        windowSizes.add(Time.days(7));
        windowSizes.add(Time.days(30));
        return windowSizes;
    }

    public static List<Time> getWindowSizesQuery2() {
        List<Time> windowSizes = new ArrayList<>();
        windowSizes.add(Time.hours(24));
        windowSizes.add(Time.days(7));
        return windowSizes;
    }
}
