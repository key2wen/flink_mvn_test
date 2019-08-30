package com.key2wen.es;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch.util.RetryRejectedExecutionFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionRequest;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
@Slf4j
public class ElasticSearchSinkUtil {

    /**
     * es sink
     *
     * @param hosts               es hosts
     * @param bulkFlushMaxActions bulk flush size
     * @param parallelism         并行数
     * @param data                数据
     * @param func
     * @param <T>
     */
    public static <T> void addSink(List<HttpHost> hosts, int bulkFlushMaxActions, int parallelism,
                                   SingleOutputStreamOperator<T> data, ElasticsearchSinkFunction<T> func) {
        ElasticsearchSink.Builder<T> esSinkBuilder = new ElasticsearchSink.Builder<>(hosts, func);
        esSinkBuilder.setBulkFlushMaxActions(bulkFlushMaxActions);

        //写入 ES 的时候会有这些情况会导致写入 ES 失败：
        //
        //1、ES 集群队列满了，报如下错误
        //12:08:07.326 [I/O dispatcher 13] ERROR o.a.f.s.c.e.ElasticsearchSinkBase - Failed Elasticsearch item request: ElasticsearchException[Elasticsearch exception [type=es_rejected_execution_exception, reason=rejected execution of org.elasticsearch.transport.TransportService$7@566c9379 on EsThreadPoolExecutor[name = node-1/write, queue capacity = 200, org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor@f00b373[Running, pool size = 4, active threads = 4, queued tasks = 200, completed tasks = 6277]]]]
        //是这样的，我电脑安装的 es 队列容量默认应该是 200，我没有修改过。我这里如果配置的 bulk flush size * 并发 sink 数量 这个值如果大于这个 queue capacity ，那么就很容易导致出现这种因为 es 队列满了而写入失败。
        //
        //当然这里你也可以通过调大点 es 的队列。参考：
        //https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-threadpool.html
        //2、ES 集群某个节点挂了
        //
        //这个就不用说了，肯定写入失败的。跟过源码可以发现 RestClient 类里的 performRequestAsync 方法一开始会随机的从集群中的某个节点进行写入数据，如果这台机器掉线，会进行重试在其他的机器上写入，那么当时写入的这台机器的请求就需要进行失败重试，否则就会把数据丢失！
        //如果你想继续让 es 写入的话就需要去重新配一下 es 让它继续写入，或者你也可以清空些不必要的数据腾出磁盘空间来。
        esSinkBuilder.setFailureHandler(new ActionRequestFailureHandler() {
            @Override
            public void onFailure(ActionRequest actionRequest, Throwable failure, int restStatusCode, RequestIndexer indexer) throws Throwable {
//                if (ExceptionUtils.containsThrowable(failure, EsRejectedExecutionException.class)) {
//                    // full queue; re-add document for indexing
//                    indexer.add(action);
//                } else if (ExceptionUtils.containsThrowable(failure, ElasticsearchParseException.class)) {
//                    // malformed document; simply drop request without failing sink
//                } else {
//                    // for all other failures, fail the sink
//                    // here the failure is simply rethrown, but users can also choose to throw custom exceptions
//                    throw failure;
//                }
            }
        });
        //如果仅仅只是想做失败重试，也可以直接使用官方提供的默认的 RetryRejectedExecutionFailureHandler ，
        // 该处理器会对 EsRejectedExecutionException 导致到失败写入做重试处理。如果你没有设置失败处理器(failure handler)，
        // 那么就会使用默认的 NoOpFailureHandler 来简单处理所有的异常。

        esSinkBuilder.setFailureHandler(new RetryRejectedExecutionFailureHandler());

        //上面代码已经可以实现你的大部分场景了，但是如果你的业务场景需要保证数据的完整性（不能出现丢数据的情况），
        // 那么就需要添加一些重试策略，因为在我们的生产环境中，很有可能会因为某些组件不稳定性导致各种问题，
        // 所以这里我们就要在数据存入失败的时候做重试操作，这里 flink 自带的 es sink 就支持了，常用的失败重试配置有:
        //1、bulk.flush.backoff.enable 用来表示是否开启重试机制
        //
        //2、bulk.flush.backoff.type 重试策略，有两种：EXPONENTIAL 指数型（表示多次重试之间的时间间隔按照指数方式进行增长）、CONSTANT 常数型（表示多次重试之间的时间间隔为固定常数）
        //
        //3、bulk.flush.backoff.delay 进行重试的时间间隔
        //
        //4、bulk.flush.backoff.retries 失败重试的次数
        //
        //5、bulk.flush.max.actions: 批量写入时的最大写入条数
        //
        //6、bulk.flush.max.size.mb: 批量写入时的最大数据量
        //
        //7、bulk.flush.interval.ms: 批量写入的时间间隔，配置后则会按照该时间间隔严格执行，无视上面的两个批量写入配置
//        esSinkBuilder.setBulkFlushBackoff(true);


        data.addSink(esSinkBuilder.build()).setParallelism(parallelism);
    }

    /**
     * 解析配置文件的 es hosts
     *
     * @param hosts
     * @return
     * @throws MalformedURLException
     */
    public static List<HttpHost> getEsAddresses(String hosts) throws MalformedURLException {
        String[] hostList = hosts.split(",");
        List<HttpHost> addresses = new ArrayList<>();
        for (String host : hostList) {
            if (host.startsWith("http")) {
                URL url = new URL(host);
                addresses.add(new HttpHost(url.getHost(), url.getPort()));
            } else {
                String[] parts = host.split(":", 2);
                if (parts.length > 1) {
                    addresses.add(new HttpHost(parts[0], Integer.parseInt(parts[1])));
                } else {
                    throw new MalformedURLException("invalid elasticsearch hosts format");
                }
            }
        }
        return addresses;
    }

//    public static class RetryRejectedExecutionFailureHandler implements ActionRequestFailureHandler {
//        private static final long serialVersionUID = -7423562912824511906L;
//
//        public RetryRejectedExecutionFailureHandler() {
//        }
//
//        @Override
//        public void onFailure(ActionRequest action, Throwable failure, int restStatusCode, RequestIndexer indexer) throws Throwable {
//            if (ExceptionUtils.findThrowable(failure, EsRejectedExecutionException.class).isPresent()) {
//                indexer.add(new ActionRequest[]{action});
//            } else {
//                if (ExceptionUtils.findThrowable(failure, SocketTimeoutException.class).isPresent()) {
//                    // 忽略写入超时，因为ElasticSearchSink 内部会重试请求，不需要抛出来去重启 flink job
//                    return;
//                } else {
//                    Optional<IOException> exp = ExceptionUtils.findThrowable(failure, IOException.class);
//                    if (exp.isPresent()) {
//                        IOException ioExp = exp.get();
//                        if (ioExp != null && ioExp.getMessage() != null && ioExp.getMessage().contains("max retry timeout")) {
//                            // request retries exceeded max retry timeout
//                            // 经过多次不同的节点重试，还是写入失败的，则忽略这个错误，丢失数据。
////                            log.error(ioExp.getMessage());
//                            return;
//                        }
//                    }
//                }
//                throw failure;
//            }
//        }
//    }
}