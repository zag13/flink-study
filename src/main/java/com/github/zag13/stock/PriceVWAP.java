package com.github.zag13.stock;

import com.github.zag13.util.stock.StockPrice;
import com.github.zag13.util.stock.StockSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

// 以5分钟为一个时间单位，计算某只股票的 VWAP 值
// VMAP = sum(price * volume) / sum(volume)
public class PriceVWAP {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StockPrice> stream = env.addSource(new StockSource("stock/stock-test.csv"));

        stream.assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<StockPrice>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.ts)
                )
                .keyBy(s -> s.symbol)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new VWAPProcessFunction())
                .print();


        env.execute("StockPriceVWAP");
    }

    public static class VWAPProcessFunction extends ProcessWindowFunction<StockPrice, Tuple2<String, Double>, String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<StockPrice> elements, Collector<Tuple2<String, Double>> out) throws Exception {
            String symbol = key;

            Double sumPrice = 0.0;
            Double sumVolume = 0.0;

            for (StockPrice stockPrice : elements) {
                sumPrice += stockPrice.price * stockPrice.volume;
                sumVolume += stockPrice.volume;
            }

            out.collect(new Tuple2<>(symbol, sumPrice / sumVolume));
        }
    }

}
