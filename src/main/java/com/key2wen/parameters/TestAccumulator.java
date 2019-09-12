package com.key2wen.parameters;

import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class TestAccumulator {

    //Accumulator(累加器)
    //我们前面已经介绍了如何将数据发送给 TaskManager，但现在我们将讨论如何从 TaskManager 中返回数据。 你可能想知道为什么我们需要做这种事情。 毕竟，Apache Flink 就是建立数据处理流水线，读取输入数据，处理数据并返回结果。
    //
    //为了表达清楚，让我们来看一个例子。假设我们需要计算每个单词在文本中出现的次数，同时我们要计算文本中有多少行：
    public void test() throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //要处理的数据集合
        DataSet<String> lines = env.fromElements("xx", "xxxx");


        // Word count 算法
        lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = line.split("\\W+");
                for (String word : words) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        })
                .groupBy(0)
                .sum(1)
                .print();

        // 计算要处理的文本中的行数
        long linesCount = lines.count();
        System.out.println(linesCount);
    }

    //问题是如果我们运行这个应用程序，它将运行两个 Flink 作业！首先得到单词统计数，然后计算行数。
    //
    //这绝对是低效的，但我们怎样才能避免这种情况呢？一种方法是使用累加器。它们允许你从 TaskManager 发送数据，并使用预定义的功能聚合此数据。 Flink 有以下内置累加器：
    //
    //IntCounter，LongCounter，DoubleCounter：允许将 TaskManager 发送的 int，long，double 值汇总在一起

    //AverageAccumulator：计算双精度值的平均值
    //
    //LongMaximum，LongMinimum，IntMaximum，IntMinimum，DoubleMaximum，DoubleMinimum：累加器，用于确定不同类型的最大值和最小值
    //
    //直方图 - 用于计算 TaskManager 的值分布
    //
    //要使用累加器，我们需要创建并注册一个用户定义的函数，然后在客户端上读取结果。下面我们来看看该如何使用呢：

    public void test2() throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //要处理的数据集合
        DataSet<String> lines = env.fromElements("xx", "xxxx");

        lines.flatMap(new RichFlatMapFunction<String, Tuple2<String, Integer>>() {

            //创建一个累加器
            private IntCounter linesNum = new IntCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                //注册一个累加器
                getRuntimeContext().addAccumulator("linesNum", linesNum);
            }

            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = line.split("\\W+");
                for (String word : words) {
                    out.collect(new Tuple2<>(word, 1));
                }

                // 处理每一行数据后 linesNum 递增
                linesNum.add(1);
            }
        })
                .groupBy(0)
                .sum(1)
                .print();

        //获取累加器结果
        int linesNum = env.getLastJobExecutionResult().getAccumulatorResult("linesNum");
        System.out.println(linesNum);
        //这样计算就可以统计输入文本中每个单词出现的次数以及它有多少行。
        //
        //如果需要自定义累加器，还可以使用 Accumulator 或 SimpleAccumulator 接口实现自己的累加器。
    }
}
