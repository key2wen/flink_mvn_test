package com.key2wen.split;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

import static com.key2wen.split.Type.*;

/**
 * http://www.54tianzhisheng.cn/2019/06/12/flink-split/
 * <p>
 * Flink 中不支持连续的 Split/Select 分流操作，
 * 要实现连续分流也可以通过其他的方式（split + filter 或者 side output）来实现
 */
public class Test {

    /**
     * 另外其实也可以通过 split + filter 组合来解决这个问题，反正关键就是不要连续的用 split 来分流。
     * <p>
     * 用 split + filter 的方案代码大概如下：
     */
    public void test() {

        SplitStream split = splitStream();

        DataStream<AlertEvent> docekr = split.select(DOCKER.id);   //选出容器的数据流

        //容器告警的数据流
        docekr.filter(new FilterFunction<AlertEvent>() {
            @Override
            public boolean filter(AlertEvent value) throws Exception {
                return !value.isRecover();
            }
        })
                .print();

        //容器恢复的数据流
        docekr.filter(new FilterFunction<AlertEvent>() {
            @Override
            public boolean filter(AlertEvent value) throws Exception {
                return value.isRecover();
            }
        })
                .print();
    }

    class AlertEvent {
        boolean recover;
        Type type;

        public boolean isRecover() {
            return recover;
        }

        public Type getType() {
            return type;
        }
    }

    public SplitStream splitStream() {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<AlertEvent> dataStream = env.fromElements();
        //dataStream 是总的数据流

        //split 是拆分后的数据流
        SplitStream<AlertEvent> split = dataStream.split(new OutputSelector<AlertEvent>() {
            @Override
            public Iterable<String> select(AlertEvent value) {
                List<String> tags = new ArrayList<>();
                switch (value.getType()) {
                    case MIDDLEWARE:
                        tags.add(MIDDLEWARE.id);
                        break;
                    case HEALTH_CHECK:
                        tags.add(HEALTH_CHECK.id);
                        break;
                    case DOCKER:
                        tags.add(DOCKER.id);
                        break;
                    //...
                    //当然这里还可以很多种类型
                }
                return tags;
            }
        });

        return split;
    }

}

enum Type {

    MIDDLEWARE("1"), HEALTH_CHECK("2"), DOCKER("3");

    String id;

    Type(String id) {
        this.id = id;
    }

}