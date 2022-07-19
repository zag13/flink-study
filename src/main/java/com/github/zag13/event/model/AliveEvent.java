package com.github.zag13.event.model;

public class AliveEvent extends BaseEvent {

    private String tag1;
    private String tag2;
    private String tag3;
    private String tag4;
    private String uniqueId;
    private String userId;
    private String serverId;
    private String roleId;
    private String roleName;
    private String roleLevel;
    private String ip;

    public AliveEvent(long eventTime, String eventType, String tag1, String tag2, String tag3, String tag4, String uniqueId, String userId, String serverId, String roleId, String roleName, String roleLevel, String ip) {
        this.eventTime = eventTime;
        this.eventType = eventType;
        this.tag1 = tag1;
        this.tag2 = tag2;
        this.tag3 = tag3;
        this.tag4 = tag4;
        this.uniqueId = uniqueId;
        this.userId = userId;
        this.serverId = serverId;
        this.roleId = roleId;
        this.roleName = roleName;
        this.roleLevel = roleLevel;
        this.ip = ip;
    }

    public static AliveEvent of(long eventTime, String eventType, String tag1, String tag2, String tag3, String tag4, String uniqueId, String userId, String serverId, String roleId, String roleName, String roleLevel, String ip) {
        return new AliveEvent(eventTime, eventType, tag1, tag2, tag3, tag4, uniqueId, userId, serverId, roleId, roleName, roleLevel, ip);
    }

}
