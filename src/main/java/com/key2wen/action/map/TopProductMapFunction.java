package com.key2wen.action.map;

import com.key2wen.action.domain.LogEntity;
import com.key2wen.action.util.LogToEntity;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author XINZE
 */
public class TopProductMapFunction implements MapFunction<String, LogEntity> {
    @Override
    public LogEntity map(String s) throws Exception {
        LogEntity log = LogToEntity.getLog(s);
        return log;
    }
}
