package com.key2wen.action.map;

import com.key2wen.action.domain.LogEntity;
import com.key2wen.action.util.LogToEntity;
import org.apache.flink.api.common.functions.MapFunction;

/*
 * 将kafka 的数据 转为 Log类
 */
public class GetLogFunction implements MapFunction<String, LogEntity> {
    @Override
    public LogEntity map(String s) throws Exception {

        LogEntity log = LogToEntity.getLog(s);
        return log;
    }
}
