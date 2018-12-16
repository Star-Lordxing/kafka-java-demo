package com.example.demo.transaction;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

public class TransactionProducer {
    private static Properties getProps(){
        Properties props =  new Properties();
        props.put("bootstrap.servers", "47.52.199.51:9092");
        props.put("retries", 2); // 重试次数
        props.put("batch.size", 100); // 批量发送大小
        props.put("buffer.memory", 33554432); // 缓存大小，根据本机内存大小配置
        props.put("linger.ms", 1000); // 发送频率，满足任务一个条件发送
        props.put("client.id", "producer-syn-2"); // 发送端id,便于统计
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("transactional.id","producer-1"); // 每台机器唯一
        props.put("enable.idempotence",true); // 设置幂等性
        return props;
    }
    public static void main(String[] args) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(getProps());
        // 初始化事务
        producer.initTransactions();
            for(int i=0;i<100;i++){
                try {
                    Thread.sleep(2000);
                    // 开启事务
                    producer.beginTransaction();
                    // 发送消息到producer-syn
                    producer.send(new ProducerRecord<String, String>("producer-syn","test3"));
                    // 发送消息到producer-asyn
                    Future<RecordMetadata> metadataFuture = producer.send(new ProducerRecord<String, String>("producer-asyn","test4"));
                    // 提交事务
                    // producer.commitTransaction();
                }catch (Exception e){
                    e.printStackTrace();
                    // 终止事务
                    producer.abortTransaction();
                }
            }
    }
}
