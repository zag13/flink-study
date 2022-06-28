package com.github.zag13.datastream.operators;

import com.github.zag13.event.model.Event;
import com.github.zag13.event.source.FileSource;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Transformations {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

//        DataStream<Event> event = env.addSource(new FileSource("event/event-test.txt"));
//        event.print();

//        DataStream<String> mapT = event.map(input -> "event time: " + input.getEventTime() + ", event type: " + input.getEventType());
//        mapT.print();

//        DataStream<String> flatMapT = event.flatMap((Event input, Collector<String> collector) -> {
//            for (String word : input.getEventType().split(" ")) {
//                collector.collect(word);
//            }
//        }).returns(Types.STRING);
//        flatMapT.print();

//        DataStream<Event> filterT = event.filter(input -> input.getEventTime() > 0);
//        filterT.print();

//        DataStream<Event> keyByT = event.keyBy(Event::getEventType);
//        keyByT.print();

//        DataStream<Event> reduceT = event.keyBy(Event::getEventType).
//                reduce((Event a, Event b) -> a.getEventTime() >= b.getEventTime() ? a : b);
//        reduceT.print();

//        DataStream<StockPrice> stockPrice = env.addSource(new StockSource("stock/stock-test.csv"));

//        DataStream<StockPrice> windowT = stockPrice.keyBy(s -> s.symbol).
//                window(TumblingProcessingTimeWindows.of(Time.seconds(5))).
//                reduce((s1, s2) -> StockPrice.of(s1.symbol, s2.price, s2.ts, s1.volume + s2.volume));
//        windowT.print();

//        DataStream<StockPrice> windowAllT = stockPrice.
//                windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5))).
//                reduce((s1, s2) -> StockPrice.of(s1.symbol, s2.price, s2.ts, s1.volume + s2.volume));
//        windowAllT.print();

//        DataStream<StockPrice> windowApplyT = stockPrice.
//                keyBy(s -> s.symbol).
//                window(TumblingProcessingTimeWindows.of(Time.seconds(5))).
//                apply(new WindowFunction<StockPrice, StockPrice, String, TimeWindow>() {
//                    @Override
//                    public void apply(String s, TimeWindow timeWindow, Iterable<StockPrice> iterable, Collector<StockPrice> collector) throws Exception {
//                        StockPrice stockPrice = iterable.iterator().next();
//                        collector.collect(StockPrice.of(stockPrice.symbol, stockPrice.price, stockPrice.ts, stockPrice.volume));
//                    }
//                }).returns(Types.POJO(StockPrice.class));
//        windowApplyT.print();

//        DataStream<Integer> allWindowApplyT = stockPrice.
//                windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5))).
//                apply(new AllWindowFunction<StockPrice, Integer, TimeWindow>() {
//                    @Override
//                    public void apply(TimeWindow timeWindow, Iterable<StockPrice> iterable, Collector<Integer> collector) throws Exception {
//                        int count = 0;
//                        for (StockPrice s : iterable) {
//                            count++;
//                        }
//                        collector.collect(count);
//                    }
//                }).returns(Types.INT);
//        allWindowApplyT.print();

//        DataStream<StockPrice> windowReduceT = stockPrice.
//                keyBy(s -> s.symbol).
//                window(TumblingProcessingTimeWindows.of(Time.seconds(5))).
//                reduce((s1, s2) -> StockPrice.of(s1.symbol, s2.price, s2.ts, s1.volume + s2.volume));
//        windowReduceT.print();

//        DataStream<StockPrice> unionT = stockPrice.union(stockPrice);
//        unionT.print();

//        DataStream<StockPrice> windowJoinT = stockPrice.
//                join(stockPrice).
//                where(s -> s.symbol).equalTo(s -> s.symbol).
//                window(TumblingProcessingTimeWindows.of(Time.seconds(5))).
//                apply(new JoinFunction<StockPrice, StockPrice, StockPrice>() {
//                    @Override
//                    public StockPrice join(StockPrice stockPrice, StockPrice stockPrice2) throws Exception {
//                        return stockPrice;
//                    }
//                });
//        windowJoinT.print();

        // ??? intervalJoin
//        DataStream<StockPrice> intervalJoinT = stockPrice.
//                keyBy(s -> s.symbol).
//                intervalJoin(stockPrice.keyBy(s -> s.symbol)).
//                between(Time.seconds(5), Time.seconds(10)).
//                process();
//        intervalJoinT.print();

        // ??? windowCoGroup
//        DataStream<StockPrice> windowCoGroupT = stockPrice.
//                coGroup(stockPrice).
//                where(s -> s.symbol).equalTo(s -> s.symbol).
//                window(TumblingProcessingTimeWindows.of(Time.seconds(5))).
//                apply(new CoGroupFunction<StockPrice, StockPrice, StockPrice>() {
//                    @Override
//                    public void coGroup(Iterable<StockPrice> iterable, Iterable<StockPrice> iterable2, Collector<StockPrice> collector) throws Exception {
//                        for (StockPrice s : iterable) {
//                            collector.collect(s);
//                        }
//                        for (StockPrice s : iterable2) {
//                            collector.collect(s);
//                        }
//                    }
//                });
//        windowCoGroupT.print();

        // Connect、CoMap、CoFlatMap

        // Iterate

        env.execute("Transformations Example");
    }

    // 实现 MapFunction
    public static class MyMapFunction implements MapFunction<Event, String> {
        @Override
        public String map(Event input) {
            return "event time: " + input.getEventTime() + ", event type: " + input.getEventType();
        }
    }

    // 实现 FlatMapFunction
    public static class MyFlatMapFunction implements FlatMapFunction<Event, String> {
        @Override
        public void flatMap(Event input, Collector<String> collector) throws Exception {
            for (String word : input.getEventType().split(" ")) {
                collector.collect(word);
            }
        }
    }

    // 实现 FilterFunction
    public static class MyFilterFunction implements FilterFunction<Event> {
        @Override
        public boolean filter(Event input) throws Exception {
            return input.getEventTime() > 0;
        }
    }

    // 实现 KeySelector
    public static class MyKeySelector implements KeySelector<Event, String> {
        @Override
        public String getKey(Event input) {
            return input.getEventType();
        }
    }

    // 实现 ReduceFunction
    public static class MyReduceFunction implements ReduceFunction<Event> {
        @Override
        public Event reduce(Event a, Event b) throws Exception {
            return a.getEventTime() >= b.getEventTime() ? a : b;
        }
    }

    // ??? 实现 WindowFunction
    // ??? 实现 WindowAllFunction
    // ??? 实现 WindowApplyFunction
    // ??? 实现 AllWindowApplyFunction
    // ??? 实现 WindowReduceFunction

    private static class todo {}

    // ------------------------   Rich...相关函数可以传参进来   ------------------------


    public static class MyRichFilterFunction extends RichFilterFunction<Event> {
        private final Integer limit;

        public MyRichFilterFunction(Integer limit) {
            this.limit = limit;
        }

        @Override
        public boolean filter(Event event) throws Exception {
            return event.getEventTime() > this.limit;
        }
    }
}
