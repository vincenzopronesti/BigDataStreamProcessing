package org.apache.flink.nyschoolbuses.util;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TimeReader {
    public static Double parseDelay(String delayString) {
        // 12/30 m
        String regex = "(\\d+)\\s*[-/]\\s*(\\d+)\\s*([mMHh])[inutesINUTESorOR].*";
        if (delayString.matches(regex)) {

            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(delayString);
            double value1;
            double value2;
            if (matcher.find()) {
                value1 = Double.parseDouble(matcher.group(1));
                value2 = Double.parseDouble(matcher.group(2));
                double avg = (value1 + value2) / 2;
                if (matcher.group(3).toLowerCase().equals("m")) {
                    return avg;
                } else if (matcher.group(3).toLowerCase().equals("h")) {
                    return avg * 60;
                }
            }
        }

        // 20/30
        regex = "(\\d+)\\s*[-/]\\s*(\\d+)\\s*";
        if (delayString.matches(regex)) {

            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(delayString);
            double value1;
            double value2;
            if (matcher.find()) {
                value1 = Double.parseDouble(matcher.group(1));
                value2 = Double.parseDouble(matcher.group(2));
                double avg = (value1 + value2) / 2;
                if (avg <= 3) {
                    return avg * 60;
                } else {
                    return avg;
                }
            }
        }

        regex = "(\\d+)\\s*([hH])[oursOURS]*\\s*/(\\d+)\\s*([hH])[oursOURS]*\\s*(\\d+)\\s*([mM])*[inutesINUTES]*";
        if (delayString.matches(regex)) {
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(delayString);
            double value1;
            double value2;
            double value3;
            if (matcher.find()) {
                value1 = Double.parseDouble(matcher.group(1));
                value1 = value1 * 60;

                value2 = Double.parseDouble(matcher.group(3));
                value2 = value2 *60;

                value3 = Double.parseDouble(matcher.group(5));
                return (value1 + value2 + value3)/2;
            }
        }

        regex = "(\\d+)\\s*([hH])[oursOURS]*\\s*(\\d+)\\s*([mM])[inutesINUTES]*";
        if (delayString.matches(regex)) {
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(delayString);
            double value1;
            double value2;
            if (matcher.find()) {
                value1 = Double.parseDouble(matcher.group(1));
                value1 = value1 * 60;

                value2 = Double.parseDouble(matcher.group(3));
                return value1 +value2;
            }
        }

        // 20 m / 30 m
        regex = "(\\d+)\\s*([mM])[inutesINUTES]*\\s*/\\s*(\\d+)\\s*([mM])[inutesINUTES]*";
        if (delayString.matches(regex)) {
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(delayString);
            double value1;
            double value2;
            if (matcher.find()) {
                value1 = Double.parseDouble(matcher.group(1));
                value2 = Double.parseDouble(matcher.group(3));
                return (value1 + value2)/2;
            }
        }

        // 1hr/30min?
        regex = "(\\d+)\\s*([hH])[oursHOURS]*\\s*/\\s*(\\d+)\\s*([mM])[inutesINUTES]*.*";
        if (delayString.matches(regex)) {
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(delayString);
            double value1;
            double value2;
            if (matcher.find()) {
                value1 = Double.parseDouble(matcher.group(1));
                value2 = Double.parseDouble(matcher.group(3));
                return value1 * 60 + value2;
            }
        }

        // 45 m / 1 h
        regex = "(\\d+)\\s*([mM])[inutesINUTES]*\\s*/\\s*(\\d+)\\s*([hH])[oursOURS]*.*";
        if (delayString.matches(regex)) {
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(delayString);
            double value1;
            double value2;
            if (matcher.find()) {
                value1 = Double.parseDouble(matcher.group(1));
                value2 = Double.parseDouble(matcher.group(3));
                return (value1 + 60 * value2)/2;
            }
        }

        // 15 -- min
        regex = "(\\d+)\\s*-*\\s*([mMhH]).*";
        if (delayString.matches(regex)) {
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(delayString);
            double value;
            if (matcher.find()) {
                value = Double.parseDouble(matcher.group(1));
                if (matcher.group(2).toLowerCase().equals("m")) {
                    return value;
                } else if (matcher.group(2).toLowerCase().equals("h")) {
                    return value * 60;
                }
            }
        }

        // 10
        regex = "\\d+";
        if (delayString.matches(regex)) {
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(delayString);
            double value;
            if (matcher.find()) {
                value = Double.parseDouble(matcher.group());
                if (value <= 3) {
                    return value * 60;
                } else if (value <= 60){
                    return value;
                }
            }
        }

        // 10 to 20 m, 10 - 20 m
        regex = "(\\d+)\\s*(-)?(to)?\\s*(\\d+)\\s*[mM].*";
        if (delayString.matches(regex)) {
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(delayString);
            double value;
            double value1;
            if (matcher.find()) {
                value = Double.parseDouble(matcher.group(1));
                value1 = Double.parseDouble(matcher.group(4));
                return (value + value1)/2;
            }
        }

        // 1:30 m
        regex = "(\\d+)\\s*:\\s*(\\d+)(\\?)?\\s*[mM].*";
        if (delayString.matches(regex)) {
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(delayString);
            double value;
            double value1;
            if (matcher.find()) {
                value = Double.parseDouble(matcher.group(1));
                value1 = Double.parseDouble(matcher.group(2));
                return (value*60 + value1);
            }
        }

        // ott-20
        regex = "([febotnvdic]*)\\s*-\\s*(\\d+)";
        if (delayString.matches(regex)) {
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(delayString);
            double val = 0;
            if (matcher.find()) {
                String month = matcher.group(1);
                double value;
                if (month.equals("feb"))
                    val = 2;
                if (month.equals("ott"))
                    val = 10;
                if (month.equals("nov"))
                    val = 11;
                if (month.equals("dic"))
                    val = 12;
                value = Double.parseDouble(matcher.group(2));
                return (value + val)/2;
            }
        }

        regex = "(\\d+)\\s*-\\s*([febotnvdic]*)";
        if (delayString.matches(regex)) {
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(delayString);
            double val = 0;
            double value;
            if (matcher.find()) {
                String month = matcher.group(2);
                if (month.equals("feb"))
                    val = 2;
                if (month.equals("ott"))
                    val = 10;
                if (month.equals("nov"))
                    val = 11;
                if (month.equals("dic"))
                    val = 12;
                value = Double.parseDouble(matcher.group(1));
                return (value + val)/2;
            }
        }

        return 0d; // null
    }

    static class Tuple<X, Y> {
        public final X x;
        public final Y y;
        public Tuple(X x, Y y) {
            this.x = x;
            this.y = y;
        }
    }

    public static void main(String[] args) {
        double THRESHOLD = .0001;
        List<Tuple<Double, String>> testData = new ArrayList<>();
        testData.add(new Tuple<Double, String> (60d,"60 MIN"));
        testData.add(new Tuple<Double, String> (15d,"15 min"));
        testData.add(new Tuple<Double, String> (15d,"15 MINS"));
        testData.add(new Tuple<Double, String> (30d,"30 mins"));
        testData.add(new Tuple<Double, String> (30d,"30MINS"));
        testData.add(new Tuple<Double, String> (45d,"45 MINUTES"));
        testData.add(new Tuple<Double, String> (20d,"20 minutes"));
        testData.add(new Tuple<Double, String> (40d,"40mins"));
        testData.add(new Tuple<Double, String> (20d,"20"));
        testData.add(new Tuple<Double, String> (20d,"20 mins."));
        testData.add(new Tuple<Double, String> (20d,"20 mns"));
        testData.add(new Tuple<Double, String> (25d,"25 MNS"));
        testData.add(new Tuple<Double, String> (50d,"50MINS"));
        testData.add(new Tuple<Double, String> (25d,"20-30 MIN"));
        testData.add(new Tuple<Double, String> (0d,"0"));
        testData.add(new Tuple<Double, String> (20d,"20 m,ins"));
        testData.add(new Tuple<Double, String> (60d,"1 hour"));
        testData.add(new Tuple<Double, String> (27.5,"25/30 MINS"));
        testData.add(new Tuple<Double, String> (22.5,"20/25 MINS"));
        testData.add(new Tuple<Double, String> (105d,"1hour45min"));
        testData.add(new Tuple<Double, String> (0d,""));
        testData.add(new Tuple<Double, String> (27.5,"25/30 MINS"));
        testData.add(new Tuple<Double, String> (17.5,"15-20 min"));
        testData.add(new Tuple<Double, String> (1.5, "01-feb"));
        testData.add(new Tuple<Double, String> (15d,"ott-20"));
        testData.add(new Tuple<Double, String> (10.5d,"10-nov"));
        testData.add(new Tuple<Double, String> (11d,"10-dic"));
        testData.add(new Tuple<Double, String> (0d,"?"));
        testData.add(new Tuple<Double, String> (75d,"1hr/1hr 30"));
        testData.add(new Tuple<Double, String> (90d,"1/2 HOUR"));
        testData.add(new Tuple<Double, String> (0d,"minutes"));
        testData.add(new Tuple<Double, String> (0d,"traffic"));
        testData.add(new Tuple<Double, String> (75d,"1 HOUR 15M"));
        testData.add(new Tuple<Double, String> (35d,"30-40mins"));
        testData.add(new Tuple<Double, String> (25d,"25--min"));
        testData.add(new Tuple<Double, String> (0d,"1310"));
        testData.add(new Tuple<Double, String> (0d,"?????????"));
        testData.add(new Tuple<Double, String> (17.5,"15 to 20m"));
        testData.add(new Tuple<Double, String> (60d,"1hr late"));
        testData.add(new Tuple<Double, String> (90d,"1:30?mins"));
        testData.add(new Tuple<Double, String> (90d,"1hr/30min?"));
        testData.add(new Tuple<Double, String> (37.5,"30min/45mi"));
        testData.add(new Tuple<Double, String> (12.5,"10-15mints"));
        testData.add(new Tuple<Double, String> (20d,"15 - 25 m"));
        testData.add(new Tuple<Double, String> (17.5,"15 to 20mi"));
        testData.add(new Tuple<Double, String> (60d,"1 HOUR"));
        testData.add(new Tuple<Double, String> (60d,"1 HR"));
        testData.add(new Tuple<Double, String> (20d,"20 min"));
        testData.add(new Tuple<Double, String> (25d,"20-30"));
        testData.add(new Tuple<Double, String> (52.5,"45min/1hr"));
        testData.add(new Tuple<Double, String> (27.5,"25-30MINS"));
        testData.add(new Tuple<Double, String> (60d,"1hr"));
        testData.add(new Tuple<Double, String> (22.5,"20-25MINS"));
        testData.add(new Tuple<Double, String> (60d,"1hr"));
        testData.add(new Tuple<Double, String> (60d,"60 MIN"));
        testData.add(new Tuple<Double, String> (75d,"1 HOUR 15M"));
        testData.add(new Tuple<Double, String> (60d,"1 HR"));
        testData.add(new Tuple<Double, String> (22.5,"20-25 min"));
        testData.add(new Tuple<Double, String> (17.5,"15-20MINS"));
        testData.add(new Tuple<Double, String> (17.5,"15-20MINS"));
        testData.add(new Tuple<Double, String> (27.5,"25-30mins"));
        testData.add(new Tuple<Double, String> (7.5,"5-10MINS"));

//        testData = new ArrayList<>();
//        testData.add(new Tuple<Double, String> (52.5d,"45min/1hr"));

        int j = 0;
        for (int i = 0; i < testData.size(); i++) {
            Double val = parseDelay(testData.get(i).y);
            if (Math.abs(val - testData.get(i).x) > THRESHOLD) {
                System.out.println("String: " + testData.get(i).y + ", Double: " + testData.get(i).x + ", Found: " + val);
                j++;
            }
        }
        System.out.println("Failed tests: " + j + "/" + testData.size());

        LocalDateTime occurredOnLocalDate = LocalDateTime.parse("2015-09-01T07:29:00.000",
                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS"));
        ZonedDateTime zdt = occurredOnLocalDate.atZone(ZoneId.of("America/New_York"));
        long timestampSec = zdt.toInstant().toEpochMilli();
        System.out.println("timestamp: " + timestampSec);
    }
}
