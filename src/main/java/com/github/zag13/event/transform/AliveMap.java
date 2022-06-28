package com.github.zag13.event.transform;

import com.github.zag13.event.model.AliveEvent;
import com.github.zag13.event.model.BaseEvent;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.Arrays;

public class AliveMap implements MapFunction<BaseEvent, AliveEvent> {

    @Override
    public AliveEvent map(BaseEvent baseEvent) throws Exception {
        String[] eventArray = baseEvent.getEventArray();
        String tag1 = eventArray[2];
        String tag2 = eventArray[3];
        String tag3 = eventArray[4];
        String tag4 = eventArray[5];
        String uniqueId = eventArray[6];
        String userId = eventArray[7];
        String serverId = eventArray[8];
        String roleId = eventArray[9];
        String roleName = eventArray[10];
        String roleLevel = eventArray[11];
        String ip = eventArray[12];
        return AliveEvent.of(baseEvent.getEventTime(), baseEvent.getEventType(), tag1, tag2, tag3, tag4, uniqueId, userId, serverId, roleId, roleName, roleLevel, ip);
    }

}


