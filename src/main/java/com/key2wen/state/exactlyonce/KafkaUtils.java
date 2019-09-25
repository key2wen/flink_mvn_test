package com.key2wen.state.exactlyonce;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 为了方便发送消息，我用一个定时任务每秒发送一个数字，1～20，往kafka写日志的程序
 */
public class KafkaUtils {
    //    private static final String broker_list = "localhost:9092";
    private static final String broker_list = "zzy:9092";
    //flink 读取kafka写入mysql exactly-once 的topic
    private static final String topic_ExactlyOnce = "mysql-exactly-Once-4";

    public static void writeToKafka2() throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        KafkaProducer producer = new KafkaProducer<String, String>(props);//老版本producer已废弃
        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);

        try {
            for (int i = 1; i <= 20; i++) {
                MysqlExactlyOncePOJO mysqlExactlyOnce = new MysqlExactlyOncePOJO(String.valueOf(i));
                ProducerRecord record = new ProducerRecord<String, String>(topic_ExactlyOnce, null, null, JSON.toJSONString(mysqlExactlyOnce));
                producer.send(record);
                System.out.println("发送数据: " + JSON.toJSONString(mysqlExactlyOnce));
                Thread.sleep(1000);
            }
        } catch (Exception e) {

        }

        producer.flush();
    }

    public static void main(String[] args) throws InterruptedException {
        writeToKafka2();
    }
}


