//package com.key2wen.es;
//
//import com.key2wen.common.model.MetricEvent;
//import com.key2wen.common.ExecutionEnvUtil;
//import com.key2wen.common.GsonUtil;
//import com.key2wen.common.KafkaConfigUtil;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.flink.api.common.functions.RuntimeContext;
//import org.apache.flink.api.java.utils.ParameterTool;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
//import org.apache.http.HttpHost;
//import org.elasticsearch.client.Requests;
//import org.elasticsearch.common.xcontent.XContentType;
//
//import java.util.List;
//
//import static com.key2wen.common.constant.PropertiesConstants.*;
//
///**
// * blog：http://www.54tianzhisheng.cn/
// * 微信公众号：zhisheng
// */
//@Slf4j
//public class Kafka2RabbitMqMain {
//    public static void main(String[] args) throws Exception {
//        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
//        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
//        DataStreamSource<MetricEvent> data = KafkaConfigUtil.buildSource(env);
//
//        List<HttpHost> esAddresses = ElasticSearchSinkUtil.getEsAddresses(parameterTool.get(ELASTICSEARCH_HOSTS));
//        int bulkSize = parameterTool.getInt(ELASTICSEARCH_BULK_FLUSH_MAX_ACTIONS, 40);
//        int sinkParallelism = parameterTool.getInt(STREAM_SINK_PARALLELISM, 5);
//
////        log.info("-----esAddresses = {}, parameterTool = {}, ", esAddresses, parameterTool);
//
//        ElasticSearchSinkUtil.addSink(esAddresses, bulkSize, sinkParallelism, data,
//                (MetricEvent metric, RuntimeContext runtimeContext, RequestIndexer requestIndexer) -> {
//                    requestIndexer.add(Requests.indexRequest()
//                            .index(ZHISHENG + "_" + metric.getName())
//                            .type(ZHISHENG)
//                            .source(GsonUtil.toJSONBytes(metric), XContentType.JSON));
//                });
//        env.execute("flink learning connectors es6");
//    }
//}