package com.github.zag13.datastream.exercise.stork_price;

import com.github.zag13.datastream.exercise.stork_price.source.StockPrice;
import com.github.zag13.datastream.exercise.stork_price.source.StockSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StockPriceFilter {
    public static void main(String[] args) throws Exception {

        // get Execution Environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(0);

        Float volumnThreshold = 200.0f;

        DataStream<StockPrice> stream = env.addSource(new StockSource("stock/stock-tick-20200108.csv"));

        DataStream<StockPrice> largeVolumnStream = stream
                .filter(stockPrice -> stockPrice.volume > volumnThreshold);

        largeVolumnStream.print();

        env.execute("stock price filter");
    }
}
