package com.github.zag13.stock.transform;

import com.github.zag13.stock.model.StockPrice;
import com.github.zag13.stock.source.StockSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// 实时计算某只股票的价格最大值
public class MaxPrice {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StockPrice> stream = env.addSource(new StockSource("stock/stock-test.csv"));

        DataStream<StockPrice> maxStream = stream.keyBy(s -> s.symbol).max("price");

        maxStream.print();

        env.execute("StockMaxPrice");
    }

}
