package com.github.zag13.datastream.exercise.stork_price;

import com.github.zag13.datastream.exercise.stork_price.source.StockPrice;
import com.github.zag13.datastream.exercise.stork_price.source.StockSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StockPriceDemo {
    public static void main(String[] args) throws Exception {

        // get Execution Environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(0);

        DataStream<StockPrice> stream = env.addSource(new StockSource("stock/stock-test.csv"));

        DataStream<StockPrice> maxStream = stream
                .keyBy(s -> s.symbol)
                .max("price");

        maxStream.print();

        env.execute("stock price");
    }
}
