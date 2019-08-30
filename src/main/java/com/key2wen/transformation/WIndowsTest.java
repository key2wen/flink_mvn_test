package com.key2wen.transformation;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * http://www.54tianzhisheng.cn/2018/11/04/Flink-Data-transformation/
 */
public class WIndowsTest {

    public void test(String hostname, int port) {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", "metric-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

        DataStreamSource<String> student = env.addSource(new FlinkKafkaConsumer011<>(
                "student",   //这个 kafka topic 需要和上面的工具类的 topic 一致
                new SimpleStringSchema(),
                props)).setParallelism(1);


        //正如命名那样，Time Windows 根据时间来聚合流数据。例如：一分钟的 tumbling time window 收集一分钟的元素，并在一分钟过后对窗口中的所有元素应用于一个函数。
        //
        //在 Flink 中定义 tumbling time windows(翻滚时间窗口) 和 sliding time windows(滑动时间窗口) 非常简单：
        //
        //tumbling time windows(翻滚时间窗口)
        //
        //输入一个时间参数
        student.keyBy(1)
                .timeWindow(Time.minutes(1)) //tumbling time window 每分钟统计一次数量和
                .sum(1);

        //sliding time windows(滑动时间窗口)
        //
        //输入两个时间参数
        student.keyBy(1)
                .timeWindow(Time.minutes(1), Time.seconds(30)) //sliding time window 每隔 30s 统计过去一分钟的数量和
                .sum(1);


        student.keyBy(1)
                .countWindow(100) //统计每 100 个元素的数量之和
                .sum(1);

        student.keyBy(1)
                .countWindow(100, 10) //每 10 个元素统计过去 100 个元素的数量之和
                .sum(1);

    }

    //Flink DataStream 程序的第一部分通常是设置基本时间特性。 该设置定义了数据流源的行为方式（例如：它们是否将分配时间戳），以及像 KeyedStream.timeWindow(Time.seconds(30)) 这样的窗口操作应该使用上面哪种时间概念。
    //
    //以下示例显示了一个 Flink 程序，该程序在每小时时间窗口中聚合事件。
    public void test2() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

// 其他
// env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
// env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

//        DataStream<MyEvent> stream = env.addSource(new FlinkKafkaConsumer09<MyEvent>(topic, schema, props));
//
//        stream
//                .keyBy((event) -> event.getUser())
//                .timeWindow(Time.hours(1))
//                .reduce((a, b) -> a.add(b))
//                .addSink(...);
    }
}
