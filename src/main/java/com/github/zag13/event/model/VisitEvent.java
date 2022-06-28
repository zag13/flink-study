package com.github.zag13.event.model;

public class VisitEvent {

    private long eventTime;
    private String eventType;
    private String tag1;
    private String tag2;
    private String deviceId;
    private String visitType;
    private String visitData;
    private String oaid;
    private String androidId;
    private String clientIp;
    private String ip;

    public VisitEvent() {
    }

    public VisitEvent(long eventTime, String eventType, String tag1, String tag2, String deviceId, String visitType, String visitData, String oaid, String androidId, String clientIp, String ip) {
        this.eventTime = eventTime;
        this.eventType = eventType;
        this.tag1 = tag1;
        this.tag2 = tag2;
        this.deviceId = deviceId;
        this.visitType = visitType;
        this.visitData = visitData;
        this.oaid = oaid;
        this.androidId = androidId;
        this.clientIp = clientIp;
        this.ip = ip;
    }

    public static VisitEvent of(long eventTime, String eventType, String tag1, String tag2, String deviceId, String visitType, String visitData, String oaid, String androidId, String clientIp, String ip) {
        return new VisitEvent(eventTime, eventType, tag1, tag2, deviceId, visitType, visitData, oaid, androidId, clientIp, ip);
    }

    public String toString() {
        return "VisitEvent{" +
                "eventTime=" + eventTime +
                ", eventType='" + eventType + '\'' +
                ", tag1='" + tag1 + '\'' +
                ", tag2='" + tag2 + '\'' +
                ", deviceId='" + deviceId + '\'' +
                ", visitType='" + visitType + '\'' +
                ", visitData='" + visitData + '\'' +
                ", oaid='" + oaid + '\'' +
                ", androidId='" + androidId + '\'' +
                ", clientIp='" + clientIp + '\'' +
                ", ip='" + ip + '\'' +
                '}';
    }
}
