package org.apache.flink.nyschoolbuses.records;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import static org.apache.flink.nyschoolbuses.util.TimeReader.parseDelay;

public class Delay {
    private String occurredOn;
    private String boro;
    private Double delay;

    public Delay() {
    }

    public Delay(String occurredOn, String boro, String delay) {
        this.occurredOn = occurredOn;
        this.boro = boro;
        this.delay = parseDelay(delay);
    }

    public long buildTS() {
        LocalDateTime occurredOnLocalDate = LocalDateTime.parse(this.occurredOn,
                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS"));
        ZonedDateTime zdt = occurredOnLocalDate.atZone(ZoneId.of("America/New_York"));
        long timestampSec = zdt.toInstant().toEpochMilli();
        return timestampSec;
    }

    public String getOccurredOn() {
        return occurredOn;
    }

    public void setOccurredOn(String occurredOn) {
        this.occurredOn = occurredOn;
    }

    public String getBoro() {
        return boro;
    }

    public void setBoro(String boro) {
        this.boro = boro;
    }

    public Double getDelay() {
        return delay;
    }

    public void setDelay(Double delay) {
        this.delay = delay;
    }

    @Override
    public String toString() {
        return "Delay{" +
                "occurredOn=" + occurredOn +
                ", boro='" + boro + '\'' +
                ", delay=" + delay +
                '}';
    }
}
