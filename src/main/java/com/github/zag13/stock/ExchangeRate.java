package com.github.zag13.stock;

import com.github.zag13.util.stock.StockPrice;
import com.github.zag13.util.stock.StockSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// 数据中股票价格以美元结算，假设美元和人民币的汇率为7，使用map()进行汇率转换，折算成人民币
public class ExchangeRate {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StockPrice> stream = env.addSource(new StockSource("stock/stock-test.csv"));

        stream.map(s -> StockPrice.of(s.symbol, s.price * 7, s.ts, s.volume)).print();

        env.execute("StockExchangeRate");
    }
}
