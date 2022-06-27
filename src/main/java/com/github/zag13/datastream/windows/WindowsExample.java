package com.github.zag13.datastream.windows;

import com.github.zag13.util.stock.StockPrice;
import com.github.zag13.util.stock.StockSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WindowsExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // countWindow() 累计单个Key中有3条数据就进行处理
        /*DataStream<Tuple2<String, Integer>> dataStream =  env.
                fromElements("haha", "haha", "hehe", "hehe", "hehe", "hihi", "hihi", "hihi").
                map((MapFunction<String, Tuple2<String, Integer>>) s -> new Tuple2<>(s, 1)).
                returns(Types.TUPLE(Types.STRING, Types.INT)).
                keyBy(t -> t.f0).
                countWindow(3).
                sum(1);

        dataStream.print();*/

        // 滚动 processing-time 窗口
        /*DataStream<StockPrice> dataStream1 = env.
                addSource(new StockSource("stock/stock-test.csv")).
                keyBy(s -> s.symbol).
                window(TumblingProcessingTimeWindows.of(Time.seconds(5))).
                reduce((s1, s2) -> StockPrice.of(s1.symbol, s2.price, s2.ts, s1.volume + s2.volume));

        dataStream1.print();*/

        // 滚动 event-time 窗口
        DataStream<StockPrice> dataStream2 = env.
                addSource(new StockSource("stock/stock-test.csv")).
                assignTimestampsAndWatermarks(WatermarkStrategy
                        .<StockPrice>forMonotonousTimestamps()
                        .withTimestampAssigner((event, timestamp) -> event.ts)
                ).
                keyBy(s -> s.symbol).
                window(TumblingEventTimeWindows.of(Time.seconds(5))).
                reduce((s1, s2) -> StockPrice.of(s1.symbol, s2.price, s2.ts, s1.volume + s2.volume));

        dataStream2.print();

        // 长度为一天的滚动 event-time 窗口， 偏移量为 -8 小时
        DataStream<StockPrice> dataStream3 = env.
                addSource(new StockSource("stock/stock-test.csv")).
                assignTimestampsAndWatermarks(WatermarkStrategy
                        .<StockPrice>forMonotonousTimestamps()
                        .withTimestampAssigner((event, timestamp) -> event.ts)
                ).
                keyBy(s -> s.symbol).
                window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8))).
                reduce((s1, s2) -> StockPrice.of(s1.symbol, s2.price, s2.ts, s1.volume + s2.volume));

        dataStream3.print();

        // 滑动 event-time 窗口
        /* input
            .keyBy(<key selector>)
            .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
            .<windowed transformation>(<window function>); */

        // 滑动 processing-time 窗口
        /* input
            .keyBy(<key selector>)
            .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
            .<windowed transformation>(<window function>); */

        // 滑动 processing-time 窗口，偏移量为 -8 小时
        /* input
            .keyBy(<key selector>)
            .window(SlidingProcessingTimeWindows.of(Time.hours(12), Time.hours(1), Time.hours(-8)))
            .<windowed transformation>(<window function>); */

        env.execute("window example");
    }

}

