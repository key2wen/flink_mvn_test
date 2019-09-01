package com.key2wen.rabbitmq;

import com.key2wen.common.ExecutionEnvUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

/**
 * 从 rabbitmq 读取数据
 */
public class Main {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameterTool = ExecutionEnvUtil.PARAMETER_TOOL;

        //这些配置建议可以放在配置文件中，然后通过 parameterTool 来获取对应的参数值
        final RMQConnectionConfig connectionConfig = new RMQConnectionConfig
                .Builder().setHost("localhost").setVirtualHost("/")
                .setPort(5672).setUserName("admin").setPassword("admin")
                .build();

        DataStreamSource<String> zhisheng = env.addSource(new RMQSource<>(connectionConfig,
                "zhisheng",
                true,
                new SimpleStringSchema()))
                .setParallelism(1);
        zhisheng.print();

        //3、该 connector（连接器）中提供了 RMQSource 类去消费 RabbitMQ queue 中的消息和确认 checkpoints 上的消息，它提供了三种不一样的保证：
        //
        //Exactly-once(只消费一次): 前提条件有，1 是要开启 checkpoint，因为只有在 checkpoint 完成后，才会返回确认消息给 RabbitMQ（这时，消息才会在 RabbitMQ 队列中删除)；2 是要使用 Correlation ID，在将消息发往 RabbitMQ 时，必须在消息属性中设置 Correlation ID。数据源根据 Correlation ID 把从 checkpoint 恢复的数据进行去重；3 是数据源不能并行，这种限制主要是由于 RabbitMQ 将消息从单个队列分派给多个消费者。
        //At-least-once(至少消费一次): 开启了 checkpoint，但未使用相 Correlation ID 或 数据源是并行的时候，那么就只能保证数据至少消费一次了
        //No guarantees(无法保证): Flink 接收到数据就返回确认消息给 RabbitMQ

        //如果想保证 exactly-once 或 at-least-once 需要把 checkpoint 开启
//        env.enableCheckpointing(10000);

        env.execute("flink learning connectors rabbitmq");
    }
    //运行 RabbitMQProducerUtil 类，再运行 Main 类！



}