package com.github.zag13.event.model;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class BaseEvent {
    protected long eventTime;
    protected String eventType;
    protected String eventString;
    protected String[] eventArray;

    public long getEventTime() {
        return eventTime;
    }

    public String getEventType() {
        return eventType;
    }

    public String getEventString() {
        return eventString;
    }

    public String[] getEventArray() {
        return eventArray;
    }

    public BaseEvent() {
    }

    public BaseEvent(long eventTime, String eventType, String eventString, String[] eventArray) {
        this.eventTime = eventTime;
        this.eventType = eventType;
        this.eventString = eventString;
        this.eventArray = eventArray;
    }

    public static BaseEvent of(String eventString) {
        String[] eventArray = eventString.split("\t");
        DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime dateTime = LocalDateTime.parse(eventArray[0], timeFormatter);
        long eventTime = Timestamp.valueOf(dateTime).getTime();
        return new BaseEvent(eventTime, eventArray[1], eventString, eventArray);
    }

    @Override
    public String toString() {
        return this.eventString;
    }
}
