package com.github.zag13.demo;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;

public class WatermarkDemo {

    public static void main(String[] args) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.addSource(new MySource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.forGenerator((ctx) -> new MyWatermarkGenerator(Duration.ofSeconds(5)))
                                .withTimestampAssigner((event, timestamp) -> Long.parseLong(event.split(",")[1]))
                                .withIdleness(Duration.ofSeconds(30))
                )
                .keyBy(i -> i.split(",")[0])
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<String, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<String> elements, Collector<String> out) throws Exception {
                        System.out.println("--------------------  START  --------------------");
                        System.out.println("Watermark：" + sdf.format(context.currentWatermark()));
                        for (String element : elements) {
                            System.out.println("Key: " + key +
                                    ", EventTime: " + sdf.format(Long.parseLong(element.split(",")[1])) +
                                    ", WindowStart: " + sdf.format(context.window().getStart()) +
                                    ", WindowEnd: " + sdf.format(context.window().getEnd()));
                        }
                        System.out.println("--------------------   END   --------------------");
                    }
                });

        env.execute("Watermark Demo");
    }

    public static class MySource implements SourceFunction<String> {
        private final String[] simulateData = new String[]{
                "Hello zag13!,1672502400000",
                "Hello zag13!,1672502403000",
                "Hello zag13!,1672502406000",
                "Hello zag13!,1672502409000",
                "Hello zag13!,1672502412000",
                "Hello zag13!,1672502415000",
                "Hello zag13!,1672502418000",
                "Hello zag13!,1672502420000",
                "Hello zag13!,1672502423000",
                "Hello zag13!,1672502427000",
                "Hello zag13!,1672502428000",
                "Hello zag13!,1672502430000",
        };
        private       boolean  isRunning    = true;
        long lastEventTs = 0;

        @Override
        public void run(SourceContext<String> sourceContext) throws Exception {
            for (String s : simulateData) {
                if (!isRunning) break;
                long currentEventTime = Long.parseLong(s.split(",")[1]);
                if (lastEventTs != 0 && lastEventTs < currentEventTime) {
                    Thread.sleep(currentEventTime - lastEventTs);
                }
                lastEventTs = currentEventTime;
                sourceContext.collect(s);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    public static class MyWatermarkGenerator implements WatermarkGenerator<String> {
        private       long maxTimestamp;
        private final long outOfOrdernessMillis;

        public MyWatermarkGenerator(Duration maxOutOfOrderness) {
            this.outOfOrdernessMillis = maxOutOfOrderness.toMillis();
            this.maxTimestamp = Long.MIN_VALUE + this.outOfOrdernessMillis + 1L;
        }

        @Override
        public void onEvent(String event, long eventTimestamp, WatermarkOutput output) {
            this.maxTimestamp = Math.max(this.maxTimestamp, eventTimestamp);

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            long eventTime = Long.parseLong(event.split(",")[1]);
            long watermark = this.maxTimestamp - this.outOfOrdernessMillis;
            System.out.println("Input: " + event + ", EventTime: " + sdf.format(eventTime) + ", Watermark: " + sdf.format(watermark));
            output.emitWatermark(new Watermark(watermark));
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
//            System.out.println("系统周期性的发射水印，CurrentTimestamp: " + System.currentTimeMillis());
        }
    }

}

