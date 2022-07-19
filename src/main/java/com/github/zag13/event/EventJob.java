package com.github.zag13.event;

import com.github.zag13.event.model.AliveEvent;
import com.github.zag13.event.model.BaseEvent;
import com.github.zag13.event.model.VisitEvent;
import com.github.zag13.event.source.FileSource;
import com.github.zag13.event.transform.AliveMap;
import com.github.zag13.event.transform.VisitMap;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class EventJob {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<BaseEvent> baseEvent = env.addSource(new FileSource("event/event-test.txt"));

        DataStream<AliveEvent> alive = baseEvent.filter(event->event.getEventType().equals("alive"))
                .map(new AliveMap());

        DataStream<VisitEvent> visit = baseEvent.filter(event->event.getEventType().equals("visit"))
                .map(new VisitMap());

//        alive.print();
//        visit.print();

        env.execute("EventJob: event");
    }

}
