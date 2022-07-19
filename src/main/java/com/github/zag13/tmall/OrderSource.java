package com.github.zag13.tmall;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * 自定义数据源，实时产出订单数据，封装至TmallOrder实例对象
 */
public class OrderSource extends RichParallelSourceFunction<TmallOrder> {
    private boolean isRunning = true;
    // 商品类别
    String category[] = {"女装", "男装", "图书", "家电", "洗护", "美妆", "运动", "游戏", "户外", "家具", "乐器", "办公"
    };

    @Override
    public void run(SourceContext<TmallOrder> ctx) throws Exception {
        Random random = new Random();
        FastDateFormat dataFormat = FastDateFormat.getInstance("yyyyMMddHHmmssSSS");
        FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss:SSS");
        while (isRunning) {
            // 创建订单
            long millis = System.currentTimeMillis();
            String orderPrice = String.format((5 + random.nextInt(100)) + ".%2d", 10 + random.nextInt(90));
            TmallOrder order = new TmallOrder(
                    String.format(dataFormat.format(millis) + "%5d", 10000 + random.nextInt(1000)), //
                    (10000 * (random.nextInt(5) + 1)) + random.nextInt(10000), //
                    Double.parseDouble(orderPrice), //
                    format.format(millis), //
                    category[random.nextInt(category.length)]
            );
            ctx.collect(order);
            TimeUnit.MILLISECONDS.sleep(100 + random.nextInt(500));
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}