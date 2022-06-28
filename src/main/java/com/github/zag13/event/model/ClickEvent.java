package com.github.zag13.event.model;

public class ClickEvent extends BaseEvent {

    private String tag1;
    private String tag2;
    private String cookieId;
    private String url;
    private String ref;
    private String ua;
    private String ip;

    public ClickEvent() {
    }

    public ClickEvent(long eventTime, String eventType, String tag1, String tag2, String cookieId, String url, String ref, String ua, String ip) {
        this.eventTime = eventTime;
        this.eventType = eventType;
        this.tag1 = tag1;
        this.tag2 = tag2;
        this.cookieId = cookieId;
        this.url = url;
        this.ref = ref;
        this.ua = ua;
        this.ip = ip;
    }

    public static ClickEvent of(long eventTime, String eventType, String tag1, String tag2, String cookieId, String url, String ref, String ua, String ip) {
        return new ClickEvent(eventTime, eventType, tag1, tag2, cookieId, url, ref, ua, ip);
    }

}
