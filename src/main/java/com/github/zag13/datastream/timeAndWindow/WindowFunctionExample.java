package com.github.zag13.datastream.timeAndWindow;

import com.github.zag13.datastream.util.stock.StockPrice;
import com.github.zag13.datastream.util.stock.StockSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;

public class WindowFunctionExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.
                addSource(new StockSource("stock/stock-test.csv")).
                keyBy(s -> s.symbol).
                // 滚动 processing-time 窗口
                        window(TumblingProcessingTimeWindows.of(Time.seconds(5))).

                // ReduceFunction 指定两条输入数据如何合并起来产生一条输出数据，输入和输出数据的类型必须相同
//                reduce((s1, s2) -> StockPrice.of(s1.symbol, s2.price, s2.ts, s1.volume + s2.volume)).

                // ReduceFunction 是 AggregateFunction 的特殊情况
//                aggregate(new AverageAggregate()).

                // ProcessWindowFunction 有能获取包含窗口内所有元素的 Iterable， 以及用来获取时间和状态信息的 Context 对象，比其他窗口函数更加灵活
                process(new FrequencyProcessWindow()).

                print();


        env.execute("Window Function Example");
    }

    // 输入数据的类型(IN)、累加器的类型（ACC）和输出数据的类型（OUT）
    public static class AverageAggregate
            implements AggregateFunction<StockPrice, Tuple3<String, Double, Integer>, Tuple2<String, Double>> {
        @Override
        public Tuple3<String, Double, Integer> createAccumulator() {
            return Tuple3.of("", 0d, 0);
        }

        @Override
        public Tuple3<String, Double, Integer> add(StockPrice item, Tuple3<String, Double, Integer> accumulator) {
            double price = accumulator.f1 + item.price;
            int count = accumulator.f2 + 1;
            return Tuple3.of(item.symbol, price, count);
        }

        @Override
        public Tuple2<String, Double> getResult(Tuple3<String, Double, Integer> accumulator) {
            return Tuple2.of(accumulator.f0, accumulator.f1 / accumulator.f2);
        }

        @Override
        public Tuple3<String, Double, Integer> merge(Tuple3<String, Double, Integer> a, Tuple3<String, Double, Integer> b) {
            return Tuple3.of(a.f0, a.f1 + b.f1, a.f2 + b.f2);
        }
    }

    // IN: 输入类型  OUT: 输出类型  KEY：Key String  W: 窗口 TimeWindow
    public static class FrequencyProcessWindow
            extends ProcessWindowFunction<StockPrice, Tuple2<String, Double>, String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<StockPrice> elements, Collector<Tuple2<String, Double>> out) {

            Map<Double, Integer> countMap = new HashMap<>();

            for (StockPrice element : elements) {
                if (countMap.containsKey(element.price)) {
                    int count = countMap.get(element.price);
                    countMap.put(element.price, count + 1);
                } else {
                    countMap.put(element.price, 1);
                }
            }

            // 按照出现次数从高到低排序
            List<Map.Entry<Double, Integer>> list = new LinkedList<>(countMap.entrySet());
            Collections.sort(list, (o1, o2) -> {
                if (o1.getValue() < o2.getValue()) {
                    return 1;
                } else {
                    return -1;
                }
            });

            // 选出出现次数最高的输出到Collector
            if (list.size() > 0) {
                out.collect(Tuple2.of(key, list.get(0).getKey()));
            }
        }
    }

}
