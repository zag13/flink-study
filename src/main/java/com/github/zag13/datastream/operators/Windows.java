package com.github.zag13.datastream.operators;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Windows {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStream<Tuple2<String, Long>> tupleStream = env.fromElements(
                Tuple2.of("abc", 0L), Tuple2.of("abc", 1L), Tuple2.of("abc", 2L),
                Tuple2.of("xyz", 0L), Tuple2.of("xyz", 1L), Tuple2.of("www", 0L)
        );
//        tupleStream.print();

        /*
        Window Assigners

        滚动窗口（Tumbling Windows）
        滚动窗口的 assigner 分发元素到指定大小的窗口。滚动窗口的大小是固定的，且各自范围之间不重叠。
        比如说，如果你指定了滚动窗口的大小为 5 分钟，那么每 5 分钟就会有一个窗口被计算，且一个新的窗口被创建。

        滑动窗口（Sliding Windows）
        滑动窗口的 assigner 分发元素到指定大小的窗口，窗口大小通过 window size 参数设置。
        滑动窗口需要一个额外的滑动距离（window slide）参数来控制生成新窗口的频率。
        因此，如果 slide 小于窗口大小，滑动窗口可以允许窗口重叠。这种情况下，一个元素可能会被分发到多个窗口。

        会话窗口（Session Windows）
        会话窗口的 assigner 会把数据按活跃的会话分组。
        与滚动窗口和滑动窗口不同，会话窗口不会相互重叠，且没有固定的开始或结束时间。
        会话窗口在一段时间没有收到数据之后会关闭，即在一段不活跃的间隔之后。
        会话窗口的 assigner 可以设置固定的会话间隔（session gap）或 用 session gap extractor 函数来动态地定义多长时间算作不活跃。
        当超出了不活跃的时间段，当前的会话就会关闭，并且将接下来的数据分发到新的会话窗口。

        全局窗口（Global Windows）
        全局窗口的 assigner 将拥有相同 key 的所有数据分发到一个全局窗口。 这样的窗口模式仅在你指定了自定义的 trigger 时有用。
        否则，计算不会发生，因为全局窗口没有天然的终点去触发其中积累的数据。
        */

        /*
        窗口函数（Window Functions）

        ReduceFunction
        ReduceFunction 指定两条输入数据如何合并起来产生一条输出数据，输入和输出数据的类型必须相同。
        Flink 使用 ReduceFunction 对窗口中的数据进行增量聚合。

        AggregateFunction
        ReduceFunction 是 AggregateFunction 的特殊情况。
        AggregateFunction 接收三个类型：输入数据的类型(IN)、累加器的类型（ACC）和输出数据的类型（OUT）。
        输入数据的类型是输入流的元素类型，AggregateFunction 接口有如下几个方法： 把每一条元素加进累加器、创建初始累加器、合并两个累加器、从累加器中提取输出（OUT 类型）。
        与 ReduceFunction 相同，Flink 会在输入数据到达窗口时直接进行增量聚合。

        ProcessWindowFunction
        ProcessWindowFunction 有能获取包含窗口内所有元素的 Iterable， 以及用来获取时间和状态信息的 Context 对象，比其他窗口函数更加灵活。
        ProcessWindowFunction 的灵活性是以性能和资源消耗为代价的， 因为窗口中的数据无法被增量聚合，而需要在窗口触发前缓存所有数据。
        ProcessWindowFunction 可以与 ReduceFunction 或 AggregateFunction 搭配使用， 使其能够在数据到达窗口的时候进行增量聚合。
        当窗口关闭时，ProcessWindowFunction 将会得到聚合的结果。 这样它就可以增量聚合窗口的元素并且从 ProcessWindowFunction 中获得窗口的元数据。
        */

        /*
        Triggers

        Trigger 决定了一个窗口（由 window assigner 定义）何时可以被 window function 处理。
        每个 WindowAssigner 都有一个默认的 Trigger。 如果默认 trigger 无法满足你的需要，你可以在 trigger(...) 调用中指定自定义的 trigger。

        Trigger 接口提供了五个方法来响应不同的事件：
            onElement() 方法在每个元素被加入窗口时调用。
            onEventTime() 方法在注册的 event-time timer 触发时调用。
            onProcessingTime() 方法在注册的 processing-time timer 触发时调用。
            onMerge() 方法与有状态的 trigger 相关。该方法会在两个窗口合并时， 将窗口对应 trigger 的状态进行合并，比如使用会话窗口时。
            最后，clear() 方法处理在对应窗口被移除时所需的逻辑。

        有两点需要注意：
            前三个方法通过返回 TriggerResult 来决定 trigger 如何应对到达窗口的事件。应对方案有以下几种：
                CONTINUE: 什么也不做
                FIRE: 触发计算
                PURGE: 清空窗口内的元素
                FIRE_AND_PURGE: 触发计算，计算结束后清空窗口内的元素
            上面的任意方法都可以用来注册 processing-time 或 event-time timer。

        Flink 包含一些内置 trigger：
            EventTimeTrigger 根据 watermark 测量的 event time 触发。
            ProcessingTimeTrigger 根据 processing time 触发。
            CountTrigger 在窗口中的元素超过预设的限制时触发。
            PurgingTrigger 接收另一个 trigger 并将它转换成一个会清理数据的 trigger。
        */

        /*
        Evictors

        Flink 的窗口模型允许在 WindowAssigner 和 Trigger 之外指定可选的 Evictor。
        Evictor 可以在 trigger 触发后、调用窗口函数之前或之后从窗口中删除元素。
        Evictor 接口提供了两个方法实现此功能：
            evictBefore() 包含在调用窗口函数前的逻辑，而 evictAfter() 包含在窗口函数调用之后的逻辑。 在调用窗口函数之前被移除的元素不会被窗口函数计算。

        Flink 内置有三个 evictor：
            CountEvictor: 仅记录用户指定数量的元素，一旦窗口中的元素超过这个数量，多余的元素会从窗口缓存的开头移除
            DeltaEvictor: 接收 DeltaFunction 和 threshold 参数，计算最后一个元素与窗口缓存中所有元素的差值， 并移除差值大于或等于 threshold 的元素。
            TimeEvictor: 接收 interval 参数，以毫秒表示。 它会找到窗口中元素的最大 timestamp max_ts 并移除比 max_ts - interval 小的所有元素。
        */

        /*
        Allowed Lateness
        默认情况下，watermark 一旦越过窗口结束的 timestamp，迟到的数据就会被直接丢弃。 但是 Flink 允许指定窗口算子最大的 allowed lateness。
        Allowed lateness 定义了一个元素可以在迟到多长时间的情况下不被丢弃，这个参数默认是 0。
        在 watermark 超过窗口末端、到达窗口末端加上 allowed lateness 之前的这段时间内到达的元素，
        依旧会被加入窗口。取决于窗口的 trigger，一个迟到但没有被丢弃的元素可能会再次触发窗口，比如 EventTimeTrigger。
        */

        tupleStream
                .keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .aggregate(new AverageAggregate(), new MyProcessWindowFunction())
                .print();

        env.execute("Windows Example");
    }

    /**
     * The accumulator is used to keep a running sum and a count. The {@code getResult} method
     * computes the average.
     */
    private static class AverageAggregate
            implements AggregateFunction<Tuple2<String, Long>, Tuple2<Long, Long>, Double> {
        @Override
        public Tuple2<Long, Long> createAccumulator() {
            return new Tuple2<>(0L, 0L);
        }

        @Override
        public Tuple2<Long, Long> add(Tuple2<String, Long> value, Tuple2<Long, Long> accumulator) {
            return new Tuple2<>(accumulator.f0 + value.f1, accumulator.f1 + 1L);
        }

        @Override
        public Double getResult(Tuple2<Long, Long> accumulator) {
            return ((double) accumulator.f0) / accumulator.f1;
        }

        @Override
        public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
            return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
        }
    }

    private static class MyProcessWindowFunction
            extends ProcessWindowFunction<Double, Tuple2<String, Double>, String, TimeWindow> {

        public void process(String key,
                            Context context,
                            Iterable<Double> averages,
                            Collector<Tuple2<String, Double>> out) {
            Double average = averages.iterator().next();
            out.collect(new Tuple2<>(key, average));
        }
    }

}
