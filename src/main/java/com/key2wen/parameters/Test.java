package com.key2wen.parameters;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;

import java.util.stream.Stream;

/**
 * http://www.54tianzhisheng.cn/2019/03/28/flink-additional-data/ 参数变量
 */
public class Test {

    public void test() throws Exception {

        DataSet<Tuple3<Long, String, String>> lines = getTuple3DataStream();


        lines.filter((FilterFunction<Tuple3<Long, String, String>>) movie -> {
            // 以“|”符号分隔电影类型
            String[] genres = movie.f2.split("\\|");

            // 查找所有 “动作” 类型的电影
            //Flink 将序列化此变量并将其与函数一起发送到集群。
            //如果你需要将大量变量传递给函数，那么这些方法就会变得非常烦人了。 为了解决这个问题，
            // Flink 提供了 withParameters 方法。 要使用它，你需要实现那些 Rich 函数，
            // 比如你不必实现 MapFunction 接口，而是实现 RichMapFunction。
            //Rich 函数允许你使用 withParameters 方法传递许多参数：
            return Stream.of(genres).anyMatch(g -> g.equals("Action"));
        }).print();
    }


    public void testRichParams() throws Exception {
        DataSet<Tuple3<Long, String, String>> lines = getTuple3DataStream();

        // Configuration 类来存储参数
        Configuration configuration = new Configuration();
        configuration.setString("genre", "Action");

        lines.filter(new FilterGenreWithParameters())
                // 将参数传递给函数

                //所有这些选项都可以使用，但如果需要为多个函数设置相同的参数，则可能会很繁琐。
                // 在 Flink 中要处理此种情况， 你可以设置所有 TaskManager 都可以访问的全局环境变量。
                //为此，首先需要使用 ParameterTool.fromArgs 从命令行读取参数
                .withParameters(configuration)
                .print();
    }


    public void testGloParam() throws Exception {

        DataSet<Tuple3<Long, String, String>> lines = getTuple3DataStream();

        //该函数将能够读取这些全局参数
        lines.filter(new FilterGenreWithGlobalEnv()) //这个函数是自己定义的
                .print();
    }

    private DataSet<Tuple3<Long, String, String>> getTuple3DataStream() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //读取命令行参数
        String[] args = new String[4];
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        ////该函数将能够读取这些全局参数
        env.getConfig().setGlobalJobParameters(parameterTool);


        //读取电影列表数据集合
        DataSet<Tuple3<Long, String, String>> lines = env.readCsvFile("movies.csv")
                .ignoreFirstLine()
                .parseQuotedStrings('"')
                .ignoreInvalidLines()
                .types(Long.class, String.class, String.class);

        return lines;
    }

}
