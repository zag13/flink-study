package com.github.zag13.datastream.eventtime;

import com.github.zag13.event.model.Event;
import com.github.zag13.event.source.FileSource;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class Watermark {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        System.out.println("defaultAutoWatermarkInterval: "+env.getConfig().getAutoWatermarkInterval());

        DataStream<Event> event = env.addSource(new FileSource("event/event.txt"))
                .filter(i -> i.getEventType().equals("event_update"))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((e, timestamp) -> e.getEventTime())
                                .withIdleness(Duration.ofSeconds(30))
                )
                .keyBy(Event::getEventType)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce((Event a, Event b) -> a.getEventTime() <= b.getEventTime() ? a : b);
        event.print();

        env.execute("Watermark Example");
    }

    /**
     * WatermarkGenerator 可以基于事件或者周期性的生成 watermark。
     * <p>
     * 注意：  WatermarkGenerator 将以前互相独立的 AssignerWithPunctuatedWatermarks
     * 和 AssignerWithPeriodicWatermarks 一同包含了进来。
     */
    public interface WatermarkGenerator<T> {

        /**
         * 每来一条事件数据调用一次，可以检查或者记录事件的时间戳，或者也可以基于事件数据本身去生成 watermark。
         */
        void onEvent(T event, long eventTimestamp, WatermarkOutput output);

        /**
         * 周期性的调用，也许会生成新的 watermark，也许不会。
         * <p>
         * 调用此方法生成 watermark 的间隔时间由 ExecutionConfig#getAutoWatermarkInterval() 决定。
         */
        void onPeriodicEmit(WatermarkOutput output);
    }

    // ————————————————————————————————    内置 Watermark 生成器   ————————————————————————————————

    /*
      forBoundedOutOfOrderness
      数据之间存在最大固定延迟的时间戳分配器

      forMonotonousTimestamps
      单调递增时间戳分配器
     */

}
