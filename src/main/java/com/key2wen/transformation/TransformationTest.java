package com.key2wen.transformation;

import com.alibaba.fastjson.JSON;
import com.key2wen.mysql.Student;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.Properties;

/**
 * http://www.54tianzhisheng.cn/2018/11/04/Flink-Data-transformation/
 */
public class TransformationTest {

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

        //1.map 这是最简单的转换之一，其中输入是一个数据流，输出的也是一个数据流：
        SingleOutputStreamOperator<Student> map = student.map(new MapFunction<String, Student>() {
            @Override
            public Student map(String value) throws Exception {
                Student s1 = JSON.parseObject(value, Student.class);
                return s1;
            }
        });
        map.print();

        //2.FlatMap 采用一条记录并输出零个，一个或多个记录。
        SingleOutputStreamOperator<Student> flatMap = student.flatMap(new FlatMapFunction<String, Student>() {
            @Override
            public void flatMap(String value, Collector<Student> out) throws Exception {
                if (value.hashCode() % 2 == 0) {
                    Student s1 = JSON.parseObject(value, Student.class);
                    out.collect(s1);
                }
            }
        });
        flatMap.print();


        //3.Filter 函数根据条件判断出结果。
        SingleOutputStreamOperator<String> filter = student.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                if (value.hashCode() > 95) {
                    return true;
                }
                return false;
            }
        });
        filter.print();


        //4.KeyBy 在逻辑上是基于 key 对流进行分区。在内部，它使用 hash 函数对流进行分区。它返回 KeyedDataStream 数据流。
        KeyedStream<String, Integer> keyBy = student.keyBy(new KeySelector<String, Integer>() {
            @Override
            public Integer getKey(String value) throws Exception {
                return Integer.valueOf(value);
            }
        });
        keyBy.print();

        //5. Reduce 返回单个的结果值，并且 reduce 操作每处理一个元素总是创建一个新值。常用的方法有 average, sum, min, max, count，使用 reduce 方法都可实现。
        SingleOutputStreamOperator<String> reduce = student.keyBy(new KeySelector<String, Integer>() {
            @Override
            public Integer getKey(String value) throws Exception {
                return value.hashCode();
            }
        }).reduce(new ReduceFunction<String>() {
            @Override
            public String reduce(String value1, String value2) throws Exception {

                return ((Integer.valueOf(value1) + Integer.valueOf(value2)) / 2) + "";
            }
        });
        reduce.print();
        //上面先将数据流进行 keyby 操作，因为执行 reduce 操作只能是 KeyedStream，然后将 String 对象做了一个求平均值的操作。

        //6。 Fold 通过将最后一个文件夹流与当前记录组合来推出 KeyedStream。 它会发回数据流。
        DataStreamSource<Integer> student2 = env.addSource(new FlinkKafkaConsumer011<>(
                "student",   //这个 kafka topic 需要和上面的工具类的 topic 一致
                new AbstractDeserializationSchema() {
                    @Override
                    public Object deserialize(byte[] bytes) throws IOException {
                        return null;
                    }
                },
                props)).setParallelism(1);
        KeyedStream<Integer, String> keyBy2 = student2.keyBy(new KeySelector<Integer, String>() {
            @Override
            public String getKey(Integer value) throws Exception {
                return value + "";
            }
        });
        keyBy2.fold("1", new FoldFunction<Integer, String>() {
            @Override
            public String fold(String accumulator, Integer value) throws Exception {
                return accumulator + "=" + value;
            }
        });


    }
}
