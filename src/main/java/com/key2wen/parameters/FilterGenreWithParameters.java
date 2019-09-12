package com.key2wen.parameters;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

import java.util.stream.Stream;

public class FilterGenreWithParameters extends RichFilterFunction<Tuple3<Long, String, String>> {

    String genre;

    //要读取这些参数，我们需要实现 “open” 方法并读取其中的参数:
    @Override
    public void open(Configuration parameters) throws Exception {
        //读取配置
        genre = parameters.getString("genre", "");
    }

    @Override
    public boolean filter(Tuple3<Long, String, String> movie) throws Exception {
        String[] genres = movie.f2.split("\\|");

        return Stream.of(genres).anyMatch(g -> g.equals(genre));
    }
}