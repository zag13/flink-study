package com.github.zag13.stock;

import com.github.zag13.util.stock.StockPrice;
import com.github.zag13.util.stock.StockSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// 过滤出交易量大于 100 的数据，生成一个大额交易数据流
public class VolumeFilter {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StockPrice> stream = env.addSource(new StockSource("stock/stock-test.csv"));

        stream.filter(s -> s.volume > 100).print();

        env.execute("StockVolumeFilter");
    }

}
