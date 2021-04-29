package com.atguigu.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @author chujian
 * @create 2021-04-28 23:16
 * 操作Kafka的工具类
 */
public class MyKafkaUtil {

    private static String kafkaServer = "hadoop102:9092,hadoop103:9092,hadoop102:9092";

    //获取FlinkKafkaConsumer
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic, String groupId) {

        //kafka连接的一些属性配置
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties);
    }
}
