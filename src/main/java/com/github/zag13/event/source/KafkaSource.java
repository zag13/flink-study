package com.github.zag13.event.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;

import java.util.Properties;
import java.util.function.Consumer;

import static com.github.zag13.util.PropertyUtil.getProperties;

public class KafkaSource {


    /*public static FlinkKafkaConsumerBase<String> getFlinkKafkaConsumer() {
        // 加载kafka.properties
        Properties kafkaProperties = getProperties("kafka.properties");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getProperty("bootstrap.servers"));
        // 可更加实际拉去数据和客户的版本等设置此值，默认30s
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, kafkaProperties.getProperty("session.timeout"));
        // 每次poll的最大数量
        // 注意该值不要改得太大，如果poll太多数据，而不能在下次poll之前消费完，则会触发一次负载均衡，产生卡顿
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, kafkaProperties.getProperty("max.records"));
        // 当前消费实例所属的消费组，请在控制台申请之后填写
        // 属于同一个组的消费实例，会负载消费消息
        props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaProperties.getProperty("group.id"));

        return new FlinkKafkaConsumer<String>(kafkaProperties.getProperty("topic"), new SimpleStringSchema(), props);
    }*/
}
