package com.key2wen.mysql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Desc:
 */
public class Main2 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new SourceFromMySQL()).print();

        env.execute("Flink add data sourc");
    }
}