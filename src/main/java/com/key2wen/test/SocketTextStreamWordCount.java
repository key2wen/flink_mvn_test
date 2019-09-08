package com.key2wen.test;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 */
public class SocketTextStreamWordCount {
    public static void main(String[] args) throws Exception {
        //参数检查
//        if (args.length != 2) {
////            System.err.println("USAGE:\nSocketTextStreamWordCount <hostname> <port>");
////            return;
////        }

        String hostname = "127.0.0.1";
        Integer port = Integer.parseInt("9000");


        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //获取数据
        DataStreamSource<String> stream = env.socketTextStream(hostname, port);

        //计数
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = stream
                .flatMap(new LineSplitter())
                .keyBy(0)

//                .countWindowAll(1l)

                .sum(1);

        sum.print();

        env.execute("Java WordCount from SocketTextStream Example");
    }

    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) {
            String[] tokens = s.toLowerCase().split("\\W+");

            for (String token : tokens) {
                if (token.length() > 0) {
                    collector.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }


    private static void kafkaTest() {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(5000); // checkpoint every 5000 msecs

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        // only required for Kafka 0.8
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "test");

        FlinkKafkaConsumer011 myConsumer = new FlinkKafkaConsumer011<>("topic", new SimpleStringSchema(), properties);

        myConsumer.setStartFromEarliest();     // start from the earliest record possible
        myConsumer.setStartFromLatest();       // start from the latest record
        myConsumer.setStartFromTimestamp(1l); // start from specified epoch timestamp (milliseconds)
        myConsumer.setStartFromGroupOffsets(); // the default behaviour


        myConsumer = new FlinkKafkaConsumer011<>(
                java.util.regex.Pattern.compile("test-topic-[0-9]"),
                new SimpleStringSchema(),
                properties);


        Map<KafkaTopicPartition, Long> specificStartOffsets = new HashMap<>();
        specificStartOffsets.put(new KafkaTopicPartition("myTopic", 0), 23L);
        specificStartOffsets.put(new KafkaTopicPartition("myTopic", 1), 31L);
        specificStartOffsets.put(new KafkaTopicPartition("myTopic", 2), 43L);

        myConsumer.setStartFromSpecificOffsets(specificStartOffsets);


//        myConsumer =
//                new FlinkKafkaConsumer011<>("topic", new SimpleStringSchema(), properties);
//        myConsumer.assignTimestampsAndWatermarks(new CustomWatermarkEmitter());

        DataStream<String> stream = env.addSource(myConsumer);


        //2.
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        DataStream<KafkaEvent> input = env
//                .addSource(
//                        new FlinkKafkaConsumer011<>(
//                                parameterTool.getRequired("input-topic"), //从参数中获取传进来的 topic
//                                new KafkaEventSchema(),
//                                parameterTool.getProperties())
//                                .assignTimestampsAndWatermarks(new CustomWatermarkExtractor()));
    }

    private static void kafkaProducer() {

        DataStream<String> stream = null;

        FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<String>(
                "localhost:9092",            // broker list
                "my-topic",                  // target topic
                new SimpleStringSchema());   // serialization schema

// versions 0.10+ allow attaching the records' event timestamp when writing them to Kafka;
// this method is not available for earlier Kafka versions
        myProducer.setWriteTimestampToKafka(true);

        stream.addSink(myProducer);
    }
}