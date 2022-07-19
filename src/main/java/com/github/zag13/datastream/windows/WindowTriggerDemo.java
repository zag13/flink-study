package com.github.zag13.datastream.windows;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;

import java.time.Duration;

public class WindowTriggerDemo {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> text = env.socketTextStream("localhost", 8801);

        text.map((MapFunction<String, Tuple3<String, Long, Integer>>) value -> {
                    String[] arr = value.split(",");
                    String id = arr[0];
                    long ts = Long.parseLong(arr[1]);
                    int i = Integer.parseInt(arr[2]);
                    return Tuple3.of(id, ts, i);
                })
                .returns(Types.TUPLE(Types.STRING, Types.LONG, Types.INT))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<String, Long, Integer>>forMonotonousTimestamps()
                                .withTimestampAssigner((x, timestamp) -> x.f1)
                )
                .keyBy(x -> x.f0)
                .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
                .trigger(ContinuousEventTimeTrigger.of(Time.seconds(5)))
                .sum(2)
                .print();

        env.execute("start demo!");

        /*
        sensor_1,1547718209000,1
        sensor_1,1547718209500,1
        sensor_1,1547718216000,1
        sensor_1,1547718217000,1
        sensor_1,1547718220000,1
        sensor_1,1547718222000,1
        sensor_1,1547718225000,1
        sensor_1,1547718227000,1
        */
    }


}


