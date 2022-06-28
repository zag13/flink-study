package com.github.zag13.event.transform;

import com.github.zag13.event.model.BaseEvent;
import com.github.zag13.event.model.VisitEvent;
import org.apache.flink.api.common.functions.MapFunction;

public class VisitMap implements MapFunction<BaseEvent, VisitEvent> {

    @Override
    public VisitEvent map(BaseEvent baseEvent) throws Exception {
        String[] eventArray = baseEvent.getEventArray();
        String tag1 = eventArray[2];
        String tag2 = eventArray[3];
        String deviceId = eventArray[4];
        String visitType = eventArray[5];
        String visitData = eventArray[6];
        String oaid = eventArray[7];
        String androidId = eventArray[8];
        String clientIp = eventArray[9];
        String ip = eventArray[10];
        return VisitEvent.of(baseEvent.getEventTime(), baseEvent.getEventType(), tag1, tag2, deviceId, visitType, visitData, oaid, androidId, clientIp, ip);
    }

}
