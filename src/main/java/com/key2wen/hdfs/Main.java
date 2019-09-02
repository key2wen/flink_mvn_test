package com.key2wen.hdfs;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.time.ZoneId;
import java.util.Properties;

public class Main {

    /**
     * 注意：batchSize和BatchRolloverInterval一定要加L，虽然不加L也不会报错，
     * 但笔者遇到一个坑设置1024*1024*1024*50 (50G)程序也不报错，但是把int类型转换为long类型，值变成了-2147483648。
     * batchsize就失效了。
     * <p>
     * BucketingSink默认是StringWriter所以不需要设置，
     * flink会根据上海时区，每天自动建立/data/twms/traffichuixing/test_topic/2919-03-06/文件夹，写入文件。
     * flink会先写入临时文件，再把临时文件变成正式文件。
     * 触发是setBatchSize，setBatchRolloverInterval，满足其中一个条件就自动转变为正式文件。
     * 我这里面设置临时文件以"."开头。具体参数可以看api设置试试
     *
     * @param args
     * @throws Exception
     */

    public static void main(String[] args) throws Exception {

        //读kafka消息初始化
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        Properties propsConsumer = new Properties();
        propsConsumer.setProperty("bootstrap.servers", "KafkaConfig.KAFKA_BROKER_LIST");
        propsConsumer.setProperty("group.id", "trafficwisdom-streaming");
        propsConsumer.put("enable.auto.commit", false);
        propsConsumer.put("max.poll.records", 1000);
        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<String>("topic_test", new SimpleStringSchema(), propsConsumer);
        consumer.setStartFromLatest();
        DataStream<String> stream = env.addSource(consumer);

        //写入hdfs
        BucketingSink<String> sink = new BucketingSink<String>("/data/twms/traffichuixing/test_topic");
        sink.setBucketer(new DateTimeBucketer<String>("yyyy-MM-dd", ZoneId.of("Asia/Shanghai")));
//        sink.setWriter(new StringWriter());
        sink.setBatchSize(1024 * 1024 * 400L); // this is 400 MB,
        sink.setBatchRolloverInterval(60 * 60 * 1000L); // this is 60 mins
        sink.setPendingPrefix("");
        sink.setPendingSuffix("");
        sink.setInProgressPrefix(".");
        stream.addSink(sink);

        env.execute("SaveToHdfs");

    }
}
