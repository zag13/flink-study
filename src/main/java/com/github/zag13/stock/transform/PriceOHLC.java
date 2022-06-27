package com.github.zag13.stock.transform;

import com.github.zag13.stock.model.StockPrice;
import com.github.zag13.stock.source.StockSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

// 以5分钟为一个时间单位，计算某只股票的 OHLC 值
// 开盘价(Open)、最高价(High)、最低价(Low)、收盘价(Close)
public class PriceOHLC {

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
                .process(new OHLCProcessFunction())
                .print();

        env.execute("StockPriceOHLC");
    }

    public static class OHLCProcessFunction extends ProcessWindowFunction<StockPrice, Tuple6<String, Double, Double, Double, Double, Integer>, String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<StockPrice> elements, Collector<Tuple6<String, Double, Double, Double, Double, Integer>> out) throws Exception {
            String symbol = key;

            Double open = elements.iterator().next().price;
            Double high = open;
            Double low = open;
            Double close = open;
            Integer count = 0;

            for (StockPrice stockPrice : elements) {
                count++;
                if (stockPrice.price > high) {
                    high = stockPrice.price;
                }
                if (stockPrice.price < low) {
                    low = stockPrice.price;
                }
                close = stockPrice.price;
            }

            out.collect(Tuple6.of(symbol, open, high, low, close, count));
        }
    }
}
