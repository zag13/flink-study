package com.github.zag13.taobao.transform;

import com.github.zag13.taobao.source.UserBehaviorSource;
import com.github.zag13.taobao.model.UserBehavior;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Objects;

// 计算每个用户当天第一次产生行为到第一次产生购买行为之间的时间差
public class UserBehaviorHesitateTime {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 如果使用Checkpoint，可以开启下面三行，Checkpoint将写入HDFS
//        env.enableCheckpointing(2000L);
//        StateBackend stateBackend = new RocksDBStateBackend("hdfs:///flink-ckp");
//        env.setStateBackend(stateBackend);

        DataStream<UserBehavior> userBehaviorStream = env.addSource(new UserBehaviorSource("taobao/UserBehavior-test.csv"))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<UserBehavior>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.timestamp * 1000)
                );

        KeyedStream<UserBehavior, Long> keyedStream = userBehaviorStream.keyBy(user -> user.userId);


        DataStream<Tuple2<Long, Long>> hesitateTimeStream = keyedStream.flatMap(new HesitateTimeFunction());

        hesitateTimeStream.print();

        env.execute("taobao UserBehavior");
    }

    public static class HesitateTimeFunction extends RichFlatMapFunction<UserBehavior, Tuple2<Long, Long>> {

        private MapState<Long, Long> behaviorMapState;

        @Override
        public void open(Configuration configuration) {
            MapStateDescriptor<Long, Long> behaviorMapStateDescriptor = new MapStateDescriptor<Long, Long>("behaviorMap", Types.LONG, Types.LONG);
            behaviorMapState = getRuntimeContext().getMapState(behaviorMapStateDescriptor);
        }

        @Override
        public void flatMap(UserBehavior input, Collector<Tuple2<Long, Long>> out) throws Exception {
            if (!behaviorMapState.contains(input.userId)) {
                behaviorMapState.put(input.userId, input.timestamp);
            }

            if (Objects.equals(input.behavior, "buy") && behaviorMapState.get(input.userId) != 0L) {
                out.collect(new Tuple2<>(input.userId, input.timestamp - behaviorMapState.get(input.userId)));
                behaviorMapState.put(input.userId, 0L);
            }
        }
    }

}
