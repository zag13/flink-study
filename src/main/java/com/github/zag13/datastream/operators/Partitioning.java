package com.github.zag13.datastream.operators;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Partitioning {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        int defaultParallelism = env.getParallelism();

        System.out.println("defaultParallelism: " + defaultParallelism);

        env.setParallelism(4);

        DataStream<Tuple2<Integer, String>> dataStream = env.fromElements(
                Tuple2.of(1, "abc"), Tuple2.of(2, "abc"),
                Tuple2.of(3, "256"), Tuple2.of(4, "xyz"),
                Tuple2.of(5, "abc"), Tuple2.of(6, "256"));


        // 使用用户定义的 Partitioner 为每个元素选择目标任务
        DataStream<Tuple2<Integer, String>> customPartitionT = dataStream.partitionCustom(
                new MyPartitioner(), tuple2 -> tuple2.f1
        );
        customPartitionT.print();

        // 将元素随机地均匀划分到分区
//        DataStream<Tuple2<Integer, String>> shuffleT = dataStream.shuffle();
//        shuffleT.print();

        // 将元素以 Round-robin 轮询的方式分发到下游算子
//        DataStream<Tuple2<Integer, String>> rescaleT = dataStream.rescale();
//        rescaleT.print();

        // 将元素广播到每个分区
//        DataStream<Tuple2<Integer, String>> broadcastT = dataStream.broadcast();
//        broadcastT.print();

        env.execute("Partitioning Example");
    }

    /**
     * Partitioner<T> 其中泛型T为指定的字段类型
     * 重写Partitioner函数，并根据T字段对数据流中的所有元素进行数据重分配
     */
    public static class MyPartitioner implements Partitioner<String> {

        private final Random rand = new Random();
        private final Pattern pattern = Pattern.compile(".*\\d+.*");

        /**
         * key 泛型T 即根据哪个字段进行数据重分配，本例中是Tuple2(Integer, String)中的String
         * numPartitions 为当前有多少个并行实例
         * 函数返回值是一个int 该元素将被发送给下游第几个实例
         */
        @Override
        public int partition(String key, int numPartitions) {
            int randomNum = rand.nextInt(numPartitions / 2);

            Matcher m = pattern.matcher(key);
            if (m.matches()) {
                return randomNum;
            } else {
                return randomNum + numPartitions / 2;
            }
        }
    }
}
