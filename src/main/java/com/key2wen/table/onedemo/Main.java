package com.key2wen.table.onedemo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;

/**
 * https://blog.csdn.net/u013411339/article/details/93267838
 */

public class Main {

    public static void main(String[] args) throws Exception {

        //第一步，创建上下文环境：
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        BatchTableEnvironment tableEnv = BatchTableEnvironment.getTableEnvironment(env);

        //第二步，读取 score.csv 并且作为 source 输入：
        DataSet<String> input = env.readTextFile("/Users/zwh/gitCode/flink_mvn_test/src/main/resources/score.csv");

        DataSet<PlayerData> topInput = input.map(new MapFunction<String, PlayerData>() {
            @Override
            public PlayerData map(String s) throws Exception {
                String[] split = s.split(",");
                return new PlayerData(String.valueOf(split[0]),
                        String.valueOf(split[1]),
                        String.valueOf(split[2]),
                        Integer.valueOf(split[3]),
                        Double.valueOf(split[4]),
                        Double.valueOf(split[5]),
                        Double.valueOf(split[6]),
                        Double.valueOf(split[7]),
                        Double.valueOf(split[8])
                );
            }
        });

        //第三步，将 source 数据注册成表：
        Table topScore = tableEnv.fromDataSet(topInput);
        tableEnv.registerTable("score", topScore);

        //第四步，核心处理逻辑 SQL 的编写：
        Table queryResult = tableEnv.sqlQuery(
                " select player, count(season) as num FROM score GROUP BY player " +
                        "ORDER BY num desc LIMIT 3 ");


        //5。结果进行打印
//        DataSet<Result> result = tableEnv.toDataSet(queryResult, Result.class);
//        result.print(); //这里没有调env.execute();也能打印，奇怪了。。

        //输出：
        //迈克尔-乔丹:10
        //凯文-杜兰特:4
        //阿伦-艾弗森:4

        //5。当然我们也可以自定义一个 Sink，将结果输出到一个文件中，例如：
        TableSink sink = new CsvTableSink("/Users/zwh/gitCode/flink_mvn_test/src/main/resources/result.csv", ",");
        String[] fieldNames = {"name", "num"};
        TypeInformation[] fieldTypes = {Types.STRING, Types.LONG};
        tableEnv.registerTableSink("result", fieldNames, fieldTypes, sink);
        queryResult.insertInto("result");

        env.execute();

    }


    public static class Result {
        public String player;
        public Long num;

        public Result() {
            super();
        }

        public Result(String player, Long num) {
            this.player = player;
            this.num = num;
        }

        @Override
        public String toString() {
            return player + ":" + num;
        }
    }
}

