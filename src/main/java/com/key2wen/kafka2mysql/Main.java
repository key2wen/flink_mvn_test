package com.key2wen.kafka2mysql;

import com.key2wen.common.GsonUtil;
import com.key2wen.mysql.Student;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Main {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", "metric-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

        SingleOutputStreamOperator<Student> student = env.addSource(new FlinkKafkaConsumer011<>(
                "student",   //这个 kafka topic 需要和上面的工具类的 topic 一致
                new SimpleStringSchema(),
                props)).setParallelism(1)
                .map(string -> GsonUtil.fromJson(string, Student.class)); //

        //因为 RichSinkFunction 中如果 sink 一条数据到 mysql 中就会调用 invoke 方法一次，所以如果要实现批量写的话，我们最好在 sink 之前就把数据聚合一下。那这里我们开个一分钟的窗口去聚合 Student 数据。
        student.timeWindowAll(Time.minutes(1)).apply(new AllWindowFunction<Student, List<Student>, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<Student> values, Collector<List<Student>> out) throws Exception {

                ArrayList<Student> students = new ArrayList<>();
                values.forEach(v -> students.add(v));

                if (students.size() > 0) {
                    System.out.println("1 分钟内收集到 student 的数据条数是：" + students.size());
                    out.collect(students);
                }
            }
        }).addSink(new SinkBatchToMySQL());

        env.execute("flink learning connectors kafka");
    }

    //下图是运行 Main 类的日志，会创建 4 个连接池是因为默认的 4 个并行度，你如果在 addSink 这个算子设置并行度为 1 的话就会创建一个连接池：
    //本文从知识星球一位朋友的疑问来写的，应该都满足了他的条件（批量/数据库连接池/写入mysql），的确网上很多的例子都是简单的 demo 形式，都是单条数据就创建数据库连接插入 MySQL，如果要写的数据量很大的话，会对 MySQL 的写有很大的压力。这也是我之前在 《从0到1学习Flink》—— Flink 写入数据到 ElasticSearch 中，数据写 ES 强调过的，如果要提高性能必定要批量的写。就拿我们现在这篇文章来说，如果数据量大的话，聚合一分钟数据达万条，那么这样批量写会比来一条写一条性能提高不知道有多少。
}