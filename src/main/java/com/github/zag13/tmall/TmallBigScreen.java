package com.github.zag13.tmall;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;

/**
 * Flink 实现：模拟简易双11实时统计大屏
 * - 实时计算出当天零点截止到当前时间的销售总额
 * - 计算出销售top3类别
 * - 每秒钟更新一次统计结果
 */
public class TmallBigScreen {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<TmallOrder> orderStream = env.addSource(new OrderSource());

        //orderStream.printToErr();

        SingleOutputStreamOperator<TmallOrder> timeStream = orderStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TmallOrder>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((e, l) -> e.getEventTime())

                );

//        timeStream.printToErr();

        // TODO：step1. 每秒统计今日各个类别销售额（window size：1d，trigger interval： 1s，keyBy：category）
        SingleOutputStreamOperator<CategoryAmount> categoryWindowStream = timeStream
                .keyBy(TmallOrder::getCategory)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .trigger(ContinuousEventTimeTrigger.of(Time.seconds(5)))
                .aggregate(
                        new AggregateFunction<TmallOrder, BigDecimal, Double>() {
                            @Override
                            public BigDecimal createAccumulator() {
                                return new BigDecimal(0);
                            }

                            @Override
                            public BigDecimal add(TmallOrder order, BigDecimal accumulator) {
                                Double orderAmount = order.getOrderAmount();
                                BigDecimal addBigDecimal = accumulator.add(new BigDecimal(orderAmount));
                                return addBigDecimal;
                            }

                            @Override
                            public BigDecimal merge(BigDecimal a, BigDecimal b) {
                                return a.add(b);
                            }

                            @Override
                            public Double getResult(BigDecimal accumulator) {
                                return accumulator.setScale(2, RoundingMode.HALF_UP).doubleValue();
                            }
                        },
                        new WindowFunction<Double, CategoryAmount, String, TimeWindow>() {
                            FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

                            @Override
                            public void apply(String key, TimeWindow window, Iterable<Double> input,
                                              Collector<CategoryAmount> out) throws Exception {
                                String category = key;
                                Double windowAmount = input.iterator().next();
                                String computeDataTime = format.format(System.currentTimeMillis());
                                out.collect(new CategoryAmount(category, windowAmount, computeDataTime));
                            }
                        }
                );

//        categoryWindowStream.printToErr();
        /*
        [2021-01-27 22:15:40]: 办公 = 120.84
        [2021-01-27 22:15:40]: 男装 = 415.24
        [2021-01-27 22:15:40]: 运动 = 319.08
        [2021-01-27 22:15:40]: 乐器 = 165.33
        [2021-01-27 22:15:40]: 户外 = 54.12
        [2021-01-27 22:15:40]: 家电 = 111.76
        [2021-01-27 22:15:40]: 图书 = 309.51
        [2021-01-27 22:15:40]: 游戏 = 92.55
        [2021-01-27 22:15:40]: 家具 = 199.87
        [2021-01-27 22:15:40]: 美妆 = 164.76
        */

        // TODO: step2. 每秒钟统计消费额Top3类别和总销售额（window：1s，processTime 处理时间）
        SingleOutputStreamOperator<String> resultStream = categoryWindowStream
                .keyBy(CategoryAmount::getComputeDateTime)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .apply(new WindowFunction<CategoryAmount, String, String, TimeWindow>() {
                    @Override
                    public void apply(String key,
                                      TimeWindow window,
                                      Iterable<CategoryAmount> input,
                                      Collector<String> out) throws Exception {
                        BigDecimal sumDecimal = new BigDecimal(0.0);
                        Queue<CategoryAmount> queue = new PriorityQueue<CategoryAmount>(
                                4, //
                                new Comparator<CategoryAmount>() {
                                    @Override
                                    public int compare(CategoryAmount o1, CategoryAmount o2) {
                                        int comp = 0;
                                        if (o1.getTotalAmount() > o2.getTotalAmount()) {
                                            comp = 1; // 升序
                                        } else if (o1.getTotalAmount() < o2.getTotalAmount()) {
                                            comp = -1; // 降序
                                        }
                                        return comp;
                                    }
                                } //
                        );
                        for (CategoryAmount element : input) {
                            System.out.println("window: " + element);
                            queue.add(element);
                            if (queue.size() > 3) queue.poll();
                            sumDecimal = sumDecimal.add(new BigDecimal(element.getTotalAmount()));
                        }
                        String computeDataTime = key;
                        double sumAmount = sumDecimal.setScale(2, RoundingMode.HALF_UP).doubleValue();
                        CategoryAmount all = new CategoryAmount("all", sumAmount, computeDataTime);
                        out.collect("All>>>>" + all.toString());
                        StringBuilder builder = new StringBuilder(computeDataTime).append(", ");
                        for (CategoryAmount item : queue) {
                            builder.append(item.toContent()).append(", ");
                        }
                        String output = builder.toString();
                        out.collect("Top3>>>>" + output.substring(0, output.length() - 2));
                    }
                });

        resultStream.printToErr();
        /*
        window: [2021-01-27 22:31:07]: 家电 = 292.35
        window: [2021-01-27 22:31:07]: 办公 = 158.36
        window: [2021-01-27 22:31:07]: 男装 = 114.84
        window: [2021-01-27 22:31:07]: 图书 = 172.78
        window: [2021-01-27 22:31:07]: 家具 = 157.0
        window: [2021-01-27 22:31:07]: 乐器 = 304.14
        window: [2021-01-27 22:31:07]: 洗护 = 101.39
        window: [2021-01-27 22:31:07]: 运动 = 313.77
        window: [2021-01-27 22:31:07]: 游戏 = 22.7
        window: [2021-01-27 22:31:07]: 女装 = 143.94
        window: [2021-01-27 22:31:07]: 户外 = 45.32
        window: [2021-01-27 22:31:07]: 美妆 = 252.45
        All>>>>[2021-01-27 22:31:07]: all = 2079.04
        Top3>>>>2021-01-27 22:31:07, 家电 = 292.35, 乐器 = 304.14, 运动 = 313.77
        */

        env.execute(TmallBigScreen.class.getSimpleName());
    }
}