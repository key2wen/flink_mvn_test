package com.key2wen.rabbitmq;

import com.key2wen.common.ExecutionEnvUtil;
import com.key2wen.common.KafkaConfigUtil;
import com.key2wen.common.model.MetricEvent;
import com.key2wen.common.schemas.MetricSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

public class Kafka2RabbitMqMain {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        DataStreamSource<MetricEvent> data = KafkaConfigUtil.buildSource(env);

        final RMQConnectionConfig connectionConfig = new RMQConnectionConfig
                .Builder().setHost("localhost").setVirtualHost("/")
                .setPort(5672).setUserName("admin").setPassword("admin")
                .build();

        //注意，换一个新的 queue，否则也会报错
        data.addSink(new RMQSink<>(connectionConfig, "zhisheng001", new MetricSchema()));
        env.execute("flink learning connectors rabbitmq");
    }
}