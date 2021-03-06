package com.github.zag13.datastream.operators;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Aggregations {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStream<Tuple3<Integer, Integer, Integer>> tupleStream = env.fromElements(
                Tuple3.of(0, 0, 0), Tuple3.of(0, 1, 1), Tuple3.of(0, 2, 2),
                Tuple3.of(1, 0, 6), Tuple3.of(1, 1, 7), Tuple3.of(1, 0, 8)
        );

        // 按第一个字段分组，对第二个字段求和，打印出来的结果如下：
        //  (0,0,0)
        //  (0,1,0)
        //  (0,3,0)
        //  (1,0,6)
        //  (1,1,6)
        //  (1,1,6)
//        DataStream<Tuple3<Integer, Integer, Integer>> sumStream = tupleStream.
//                keyBy(tuple -> tuple.f0).
//                sum(1);
//        sumStream.print();

        // 按第一个字段分组，对第三个字段求最大值，使用max()，打印出来的结果如下：
        //  (0,0,0)
        //  (0,0,1)
        //  (0,0,2)
        //  (1,0,6)
        //  (1,0,7)
        //  (1,0,8)
//        DataStream<Tuple3<Integer, Integer, Integer>> maxStream = tupleStream.
//                keyBy(tuple -> tuple.f0).
//                max(2);
//        maxStream.print();

        // 按第一个字段分组，对第三个字段求最大值，使用maxBy()，打印出来的结果如下：
        //  (0,0,0)
        //  (0,1,1)
        //  (0,2,2)
        //  (1,0,6)
        //  (1,1,7)
        //  (1,0,8)
//        DataStream<Tuple3<Integer, Integer, Integer>> maxByStream = tupleStream.
//                keyBy(tuple -> tuple.f0).
//                maxBy(2);
//        maxByStream.print();

        env.execute("Aggregations Example");
    }

}
