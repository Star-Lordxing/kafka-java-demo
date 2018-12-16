package com.example.demo.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class FireProducer {
    private static Properties getProps(){
        Properties props =  new Properties();
        props.put("bootstrap.servers", "47.52.199.51:9092");
        props.put("acks", "all"); // 发送所有ISR
        props.put("retries", 2); // 重试次数
        props.put("batch.size", 16384); // 批量发送大小
        props.put("buffer.memory", 33554432); // 缓存大小，根据本机内存大小配置
        props.put("linger.ms", 1000); // 发送频率，满足任务一个条件发送
        props.put("client.id", "producer-syn-1"); // 发送端id,便于统计
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    public static void main(String[] args) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(getProps());
        for(int i=0; i< 1000; i++){
            ProducerRecord<String, String> record = new ProducerRecord<>("producer-syn", "topic_"+i,"producer-syn-"+i);
            // 不关心发送结果
            producer.send(record);
        }

        producer.flush();
        producer.close();
    }
}
