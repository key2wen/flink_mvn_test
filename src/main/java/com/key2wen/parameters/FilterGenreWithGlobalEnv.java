package com.key2wen.parameters;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.stream.Stream;

class FilterGenreWithGlobalEnv extends RichFilterFunction<Tuple3<Long, String, String>> {

    @Override
    public boolean filter(Tuple3<Long, String, String> movie) throws Exception {

        String[] genres = movie.f2.split("\\|");
        //要读取配置，我们需要调用 getGlobalJobParameter 来获取所有全局参数，然后使用 get 方法获取我们要的参数。

        //获取全局的配置
        ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

        //读取配置
        String genre = parameterTool.get("genre");

        return Stream.of(genres).anyMatch(g -> g.equals(genre));
    }
}