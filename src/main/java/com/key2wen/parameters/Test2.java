package com.key2wen.parameters;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.io.File;
import java.util.Collection;

public class Test2 {

    //如果你想将数据从客户端发送到 TaskManager，上面文章(Test类)中讨论的方法都适合你，但如果数据以数据集的形式存在于 TaskManager 中，
    // 该怎么办？ 在这种情况下，最好使用 Flink 中的另一个功能 —— 广播变量。 它只允许将数据集发送给那些执行你 Job 里面函数的任务管理器。
    //
    //假设我们有一个数据集，其中包含我们在进行文本处理时应忽略的单词，并且我们希望将其设置为我们的函数。
    // 要为单个函数设置广播变量，我们需要使用 withBroadcastSet 方法和数据集。
    public void test2() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Integer> toBroadcast = env.fromElements(1, 2, 3);
        // 获取要忽略的单词集合
        DataSet<String> wordsToIgnore = env.fromElements("xx", "gg");

        DataSet<String> data = env.fromElements("xx", "xxxx");

        data.flatMap(new RichFlatMapFunction<String, Tuple2<String, Integer>>() {

            // 存储要忽略的单词集合. 这将存储在 TaskManager 的内存中
            Collection<String> wordsToIgnore;

            @Override
            public void open(Configuration parameters) throws Exception {
                //读取要忽略的单词的集合
                wordsToIgnore = getRuntimeContext().getBroadcastVariable("wordsToIgnore");
            }

            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = line.split("\\W+");
                for (String word : words)
                    //使用要忽略的单词集合
                    if (wordsToIgnore.contains(word))
                        out.collect(new Tuple2<String, Integer>(word, 1));
            }
            //通过广播变量传递数据集
        }).withBroadcastSet(wordsToIgnore, "wordsToIgnore");

        //你应该记住，如果要使用广播变量，那么数据集将会存储在 TaskManager 的内存中，如果数据集和越大，那么占用的内存就会越大，因此使用广播变量适用于较小的数据集。
    }

    //如果要向每个 TaskManager 发送更多数据并且不希望将这些数据存储在内存中，可以使用 Flink 的分布式缓存向
    // TaskManager 发送静态文件。 要使用 Flink 的分布式缓存，你首先需要将文件存储在一个分布式文件系统（如 HDFS）中，然后在缓存中注册该文件：

    public void test3() throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //从 HDFS 注册文件
        env.registerCachedFile("hdfs:///path/to/file", "machineLearningModel");


        env.execute();
    }

    //为了访问分布式缓存，我们需要实现一个 Rich 函数：
    class MyClassifier extends RichMapFunction<String, Integer> {

        @Override
        public void open(Configuration config) {
            File machineLearningModel = getRuntimeContext().
                    getDistributedCache().getFile("machineLearningModel");
            //...

            // 请注意，要访问分布式缓存中的文件，我们需要使用我们用于注册文件的 key，
            // 比如上面代码中的 machineLearningModel。


        }

        @Override
        public Integer map(String value) throws Exception {
            //...
            return null;
        }
    }

}
