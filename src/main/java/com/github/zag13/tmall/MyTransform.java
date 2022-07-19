package com.github.zag13.tmall;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class MyTransform {

    public static class Agg1 implements AggregateFunction<TmallOrder, BigDecimal, Double> {

        @Override
        public BigDecimal createAccumulator() {
            return new BigDecimal(0);
        }

        @Override
        public BigDecimal add(TmallOrder order, BigDecimal accumulator) {
            Double orderAmount = order.getOrderAmount();
            return accumulator.add(new BigDecimal(orderAmount));
        }

        @Override
        public Double getResult(BigDecimal accumulator) {
            return accumulator.setScale(2, RoundingMode.HALF_UP).doubleValue();
        }

        @Override
        public BigDecimal merge(BigDecimal a, BigDecimal b) {
            return a.add(b);
        }
    }

    public static class WinFunc1 implements WindowFunction<Double, CategoryAmount, String, TimeWindow> {
        FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

        @Override
        public void apply(String key, TimeWindow window, Iterable<Double> input,
                          Collector<CategoryAmount> out) throws Exception {
            // 类别category
            String category = key;
            // 窗口中消费金额
            Double windowAmount = input.iterator().next();
            // 窗口结束时间
            String computeDataTime = format.format(System.currentTimeMillis());
            // 输出结果
            out.collect(new CategoryAmount(category, windowAmount, computeDataTime));
        }
    }
}
