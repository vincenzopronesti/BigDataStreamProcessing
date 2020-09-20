package org.apache.flink.nyschoolbuses.records;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Locale;

public class DelayReason {
    private String occurredOn;
    private String reason;

    public DelayReason() {
    }

    public DelayReason(String occurredOn, String reason) {
        this.occurredOn = occurredOn;
        this.reason = reason;
    }

    public long buildTS() {
        LocalDateTime occurredOnLocalDate = LocalDateTime.parse(this.occurredOn, DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS"));
        ZonedDateTime zdt = occurredOnLocalDate.atZone(ZoneId.of("America/New_York"));
        long timestampSec = zdt.toInstant().toEpochMilli();
        return timestampSec;
    }
    
    public boolean isSlotA() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss", Locale.US);
        Date timestamp;
        Date hoursMin = null;
        try {
            timestamp = sdf.parse(this.occurredOn);
            String eventTimeString = new SimpleDateFormat("HH:mm").format(timestamp);
            hoursMin = new SimpleDateFormat("HH:mm").parse(eventTimeString);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        String SLOT_A_START = "05:00";
        String SLOT_A_END = "11:59";

        Date startSlotA = null;
        Date endSlotA = null;
        try {
            startSlotA = new SimpleDateFormat("HH:mm").parse(SLOT_A_START);
            endSlotA = new SimpleDateFormat("HH:mm").parse(SLOT_A_END);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        if (hoursMin.equals(startSlotA) || hoursMin.equals(endSlotA) ||
                (hoursMin.after(startSlotA) && hoursMin.before(endSlotA)))
                return true;
        return false;
    }

    public boolean isSlotB() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss", Locale.US);
        Date timestamp;
        Date hoursMin = null;
        try {
            timestamp = sdf.parse(this.occurredOn);
            String eventTimeString = new SimpleDateFormat("HH:mm").format(timestamp);
            hoursMin = new SimpleDateFormat("HH:mm").parse(eventTimeString);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        String SLOT_B_START = "12:00";
        String SLOT_B_END = "19:00";

        Date startSlotB = null;
        Date endSlotB = null;
        try {
            startSlotB = new SimpleDateFormat("HH:mm").parse(SLOT_B_START);
            endSlotB = new SimpleDateFormat("HH:mm").parse(SLOT_B_END);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        if (hoursMin.equals(startSlotB) || hoursMin.equals(endSlotB) ||
                (hoursMin.after(startSlotB) && hoursMin.before(endSlotB)))
            return true;
        return false;
    }

    public String getOccurredOn() {
        return occurredOn;
    }

    public void setOccurredOn(String occurredOn) {
        this.occurredOn = occurredOn;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    @Override
    public String toString() {
        return "DelayReason{" +
                "occurredOn=" + occurredOn +
                ", reason=" + reason +
                '}';
    }
}
