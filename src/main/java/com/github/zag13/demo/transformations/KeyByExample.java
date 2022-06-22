package com.github.zag13.demo.transformations;

import com.github.zag13.demo.types.Word;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyByExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<Integer, Double>> dataStream = senv.fromElements(
                Tuple2.of(1, 1.0), Tuple2.of(2, 3.2), Tuple2.of(1, 5.5),
                Tuple2.of(3, 10.0), Tuple2.of(3, 12.5));

        // 使用数字位置定义Key 按照第一个字段进行分组
        // Flink 1.11开始废弃了这个方法
        DataStream<Tuple2<Integer, Double>> keyedStream = dataStream.keyBy(0).sum(1);
//        keyedStream.print();

        DataStream<Word> wordStream = senv.fromElements(
                Word.of("Hello", 1), Word.of("Flink", 1),
                Word.of("Hello", 2), Word.of("Flink", 2)
        );

        // 使用字段名
        // Flink 1.11开始废弃了这个方法
        DataStream<Word> fieldNameStream = wordStream.keyBy("word").sum("count");
        fieldNameStream.print();

        // Flink 1.11之后主推基于KeySelector的方法，类型安全
        // 使用Lambda表达式构建 KeySelector
        DataStream<Word> keyByLambdaStream = wordStream.keyBy(w -> w.word).sum("count");
//        keyByLambdaStream.print();

        // 使用KeySelector
        DataStream<Word> keySelectorStream = wordStream.keyBy(new KeySelector<Word, String> () {
            @Override
            public String getKey(Word in) {
                return in.word;
            }
        }).sum("count");
//        keySelectorStream.print();

        senv.execute("basic keyBy transformation");
    }

    public static class MyKeySelector implements KeySelector<Tuple2<Integer, Double>, Integer> {

        @Override
        public Integer getKey(Tuple2<Integer, Double> in) {
            return in.f0;
        }

    }
}
