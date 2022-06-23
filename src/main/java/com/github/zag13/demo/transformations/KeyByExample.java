package com.github.zag13.demo.transformations;

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

        // 使用 Lambda表达式 构建 KeySelector
        DataStream<Tuple2<Integer, Double>> keyedStream = dataStream.
                keyBy(tuple2 -> tuple2.f0).
                sum(1);
//        keyedStream.print();

        // 自定义 KeySelector
        DataStream<Tuple2<Integer, Double>> keyedStream2 = dataStream.
                keyBy(new MyKeySelector()).
                sum(1);
        keyedStream2.print();

        DataStream<Word> wordStream = senv.fromElements(
                Word.of("Hello", 1), Word.of("Flink", 1),
                Word.of("Hello", 2), Word.of("Flink", 2)
        );

        DataStream<Word> keyByLambdaStream = wordStream.
                keyBy(w -> w.word).
                sum("count");
//        keyByLambdaStream.print();

        // 使用 匿名 KeySelector
        DataStream<Word> keySelectorStream = wordStream.
                keyBy(new KeySelector<Word, String>() {
                    @Override
                    public String getKey(Word in) {
                        return in.word;
                    }
                }).
                sum("count");
//        keySelectorStream.print();

        DataStream<Word> keySelectorStream2 = wordStream.
                keyBy((KeySelector<Word, String>) in -> in.word).
                sum("count");
//        keySelectorStream2.print();

        senv.execute("basic keyBy transformation");
    }

    public static class MyKeySelector implements KeySelector<Tuple2<Integer, Double>, Integer> {

        @Override
        public Integer getKey(Tuple2<Integer, Double> in) {
            return in.f0;
        }

    }

    public static class Word {

        public String word;
        public int count;

        public Word() {
        }

        public Word(String word, int count) {
            this.word = word;
            this.count = count;
        }

        public static Word of(String word, int count) {
            return new Word(word, count);
        }

        @Override
        public String toString() {
            return this.word + ": " + this.count;
        }
    }
}
