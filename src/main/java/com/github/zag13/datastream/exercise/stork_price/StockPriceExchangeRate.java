package com.github.zag13.datastream.exercise.stork_price;

import com.github.zag13.datastream.exercise.stork_price.source.StockPrice;
import com.github.zag13.datastream.exercise.stork_price.source.StockSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StockPriceExchangeRate {
    public static void main(String[] args) throws Exception {

        // 创建Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(0);

        DataStream<StockPrice> stream = env.addSource(new StockSource("stock/stock-tick-20200108.csv"));

        DataStream<StockPrice> exchangeRateStream = stream
                .map(stockPrice ->
                        StockPrice.of(stockPrice.symbol, stockPrice.price * 7, stockPrice.ts, stockPrice.volume));

        exchangeRateStream.print();

        env.execute("stock price exchange rate");
    }
}
