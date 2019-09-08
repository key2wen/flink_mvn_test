package com.key2wen.action;

import com.key2wen.action.agg.CountAgg;
import com.key2wen.action.domain.LogEntity;
import com.key2wen.action.domain.TopProductEntity;
import com.key2wen.action.map.TopProductMapFunction;
import com.key2wen.action.top.TopNHotItems;
import com.key2wen.action.window.WindowResultFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * 热门商品 -> redis
 *
 * @author XINZE
 */
public class TopProductTask2 {

    private static final int topSize = 5;

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 开启EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //获取数据
        DataStreamSource<String> dataStream = env.socketTextStream("127.0.0.1", 9000);

        DataStream<TopProductEntity> topProduct = dataStream.map(new TopProductMapFunction()).

                //AscendingTimestampExtractor: https://my.oschina.net/go4it/blog/2991005
                // 抽取时间戳做watermark 以 秒 为单位
                        assignTimestampsAndWatermarks(new AscendingTimestampExtractor<LogEntity>() {
                    @Override
                    public long extractAscendingTimestamp(LogEntity logEntity) {
                        return logEntity.getTime() * 1000;
                    }
                })

                // 按照productId 按滑动窗口
                .keyBy("productId").timeWindow(Time.seconds(60), Time.seconds(5))
//                .sum("xx")
                .aggregate(new CountAgg(), new WindowResultFunction())

                .keyBy("windowEnd")
                .process(new TopNHotItems(topSize)).flatMap(new FlatMapFunction<List<String>, TopProductEntity>() {
                    @Override
                    public void flatMap(List<String> strings, Collector<TopProductEntity> collector) throws Exception {
                        System.out.println("-------------Top N Product------------");
                        for (int i = 0; i < strings.size(); i++) {
                            TopProductEntity top = new TopProductEntity();
                            top.setRankName(String.valueOf(i));
                            top.setProductId(Integer.parseInt(strings.get(i)));
                            // 输出排名结果
                            System.out.println(top);
                            collector.collect(top);
                        }

                    }
                });

        topProduct.print();

        env.execute("Top N ");
    }
}
//
//-------------Top N Product------------
//TopProductEntity{productId=13, actionTimes=0, windowEnd=0, rankName='0'}
//TopProductEntity{productId=13, actionTimes=0, windowEnd=0, rankName='0'}
//TopProductEntity{productId=12, actionTimes=0, windowEnd=0, rankName='1'}
//TopProductEntity{productId=12, actionTimes=0, windowEnd=0, rankName='1'}
//TopProductEntity{productId=11, actionTimes=0, windowEnd=0, rankName='2'}
//TopProductEntity{productId=11, actionTimes=0, windowEnd=0, rankName='2'}
//1,14,1567952344,3
//1,14,1567952345,1
//1,14,1567952411,2
//-------------Top N Product------------
//TopProductEntity{productId=13, actionTimes=0, windowEnd=0, rankName='0'}
//TopProductEntity{productId=13, actionTimes=0, windowEnd=0, rankName='0'}
//TopProductEntity{productId=12, actionTimes=0, windowEnd=0, rankName='1'}
//TopProductEntity{productId=12, actionTimes=0, windowEnd=0, rankName='1'}
//TopProductEntity{productId=11, actionTimes=0, windowEnd=0, rankName='2'}
//TopProductEntity{productId=11, actionTimes=0, windowEnd=0, rankName='2'}
//TopProductEntity{productId=14, actionTimes=0, windowEnd=0, rankName='3'}
//TopProductEntity{productId=14, actionTimes=0, windowEnd=0, rankName='3'}
//-------------Top N Product------------
//TopProductEntity{productId=13, actionTimes=0, windowEnd=0, rankName='0'}
//TopProductEntity{productId=13, actionTimes=0, windowEnd=0, rankName='0'}
//TopProductEntity{productId=12, actionTimes=0, windowEnd=0, rankName='1'}
//TopProductEntity{productId=12, actionTimes=0, windowEnd=0, rankName='1'}
//TopProductEntity{productId=14, actionTimes=0, windowEnd=0, rankName='2'}
//TopProductEntity{productId=14, actionTimes=0, windowEnd=0, rankName='2'}
//TopProductEntity{productId=11, actionTimes=0, windowEnd=0, rankName='3'}
//TopProductEntity{productId=11, actionTimes=0, windowEnd=0, rankName='3'}
//-------------Top N Product------------
//TopProductEntity{productId=13, actionTimes=0, windowEnd=0, rankName='0'}
//TopProductEntity{productId=13, actionTimes=0, windowEnd=0, rankName='0'}
//TopProductEntity{productId=12, actionTimes=0, windowEnd=0, rankName='1'}
//TopProductEntity{productId=12, actionTimes=0, windowEnd=0, rankName='1'}
//TopProductEntity{productId=14, actionTimes=0, windowEnd=0, rankName='2'}
//TopProductEntity{productId=14, actionTimes=0, windowEnd=0, rankName='2'}
//TopProductEntity{productId=11, actionTimes=0, windowEnd=0, rankName='3'}
//TopProductEntity{productId=11, actionTimes=0, windowEnd=0, rankName='3'}
//-------------Top N Product------------
//TopProductEntity{productId=13, actionTimes=0, windowEnd=0, rankName='0'}
//TopProductEntity{productId=13, actionTimes=0, windowEnd=0, rankName='0'}
//TopProductEntity{productId=12, actionTimes=0, windowEnd=0, rankName='1'}
//TopProductEntity{productId=12, actionTimes=0, windowEnd=0, rankName='1'}
//TopProductEntity{productId=14, actionTimes=0, windowEnd=0, rankName='2'}
//TopProductEntity{productId=14, actionTimes=0, windowEnd=0, rankName='2'}
//TopProductEntity{productId=11, actionTimes=0, windowEnd=0, rankName='3'}
//TopProductEntity{productId=11, actionTimes=0, windowEnd=0, rankName='3'}
//-------------Top N Product------------
//TopProductEntity{productId=13, actionTimes=0, windowEnd=0, rankName='0'}
//TopProductEntity{productId=13, actionTimes=0, windowEnd=0, rankName='0'}
//TopProductEntity{productId=12, actionTimes=0, windowEnd=0, rankName='1'}
//TopProductEntity{productId=12, actionTimes=0, windowEnd=0, rankName='1'}
//TopProductEntity{productId=14, actionTimes=0, windowEnd=0, rankName='2'}
//TopProductEntity{productId=14, actionTimes=0, windowEnd=0, rankName='2'}
//TopProductEntity{productId=11, actionTimes=0, windowEnd=0, rankName='3'}
//TopProductEntity{productId=11, actionTimes=0, windowEnd=0, rankName='3'}
//-------------Top N Product------------
//TopProductEntity{productId=13, actionTimes=0, windowEnd=0, rankName='0'}
//TopProductEntity{productId=13, actionTimes=0, windowEnd=0, rankName='0'}
//TopProductEntity{productId=12, actionTimes=0, windowEnd=0, rankName='1'}
//TopProductEntity{productId=12, actionTimes=0, windowEnd=0, rankName='1'}
//TopProductEntity{productId=14, actionTimes=0, windowEnd=0, rankName='2'}
//TopProductEntity{productId=14, actionTimes=0, windowEnd=0, rankName='2'}
//TopProductEntity{productId=11, actionTimes=0, windowEnd=0, rankName='3'}
//TopProductEntity{productId=11, actionTimes=0, windowEnd=0, rankName='3'}
//-------------Top N Product------------
//TopProductEntity{productId=13, actionTimes=0, windowEnd=0, rankName='0'}
//TopProductEntity{productId=13, actionTimes=0, windowEnd=0, rankName='0'}
//TopProductEntity{productId=12, actionTimes=0, windowEnd=0, rankName='1'}
//TopProductEntity{productId=12, actionTimes=0, windowEnd=0, rankName='1'}
//TopProductEntity{productId=14, actionTimes=0, windowEnd=0, rankName='2'}
//TopProductEntity{productId=14, actionTimes=0, windowEnd=0, rankName='2'}
//TopProductEntity{productId=11, actionTimes=0, windowEnd=0, rankName='3'}
//TopProductEntity{productId=11, actionTimes=0, windowEnd=0, rankName='3'}
//-------------Top N Product------------
//TopProductEntity{productId=13, actionTimes=0, windowEnd=0, rankName='0'}
//TopProductEntity{productId=13, actionTimes=0, windowEnd=0, rankName='0'}
//TopProductEntity{productId=12, actionTimes=0, windowEnd=0, rankName='1'}
//TopProductEntity{productId=12, actionTimes=0, windowEnd=0, rankName='1'}
//TopProductEntity{productId=14, actionTimes=0, windowEnd=0, rankName='2'}
//TopProductEntity{productId=14, actionTimes=0, windowEnd=0, rankName='2'}
//TopProductEntity{productId=11, actionTimes=0, windowEnd=0, rankName='3'}
//TopProductEntity{productId=11, actionTimes=0, windowEnd=0, rankName='3'}
//-------------Top N Product------------
//TopProductEntity{productId=13, actionTimes=0, windowEnd=0, rankName='0'}
//TopProductEntity{productId=13, actionTimes=0, windowEnd=0, rankName='0'}
//TopProductEntity{productId=12, actionTimes=0, windowEnd=0, rankName='1'}
//TopProductEntity{productId=12, actionTimes=0, windowEnd=0, rankName='1'}
//TopProductEntity{productId=14, actionTimes=0, windowEnd=0, rankName='2'}
//TopProductEntity{productId=14, actionTimes=0, windowEnd=0, rankName='2'}
//TopProductEntity{productId=11, actionTimes=0, windowEnd=0, rankName='3'}
//TopProductEntity{productId=11, actionTimes=0, windowEnd=0, rankName='3'}
//-------------Top N Product------------
//TopProductEntity{productId=13, actionTimes=0, windowEnd=0, rankName='0'}
//TopProductEntity{productId=13, actionTimes=0, windowEnd=0, rankName='0'}
//TopProductEntity{productId=12, actionTimes=0, windowEnd=0, rankName='1'}
//TopProductEntity{productId=12, actionTimes=0, windowEnd=0, rankName='1'}
//TopProductEntity{productId=14, actionTimes=0, windowEnd=0, rankName='2'}
//TopProductEntity{productId=14, actionTimes=0, windowEnd=0, rankName='2'}
//TopProductEntity{productId=11, actionTimes=0, windowEnd=0, rankName='3'}
//TopProductEntity{productId=11, actionTimes=0, windowEnd=0, rankName='3'}
//-------------Top N Product------------
//TopProductEntity{productId=13, actionTimes=0, windowEnd=0, rankName='0'}
//TopProductEntity{productId=13, actionTimes=0, windowEnd=0, rankName='0'}
//TopProductEntity{productId=14, actionTimes=0, windowEnd=0, rankName='1'}
//TopProductEntity{productId=14, actionTimes=0, windowEnd=0, rankName='1'}
//-------------Top N Product------------
//TopProductEntity{productId=14, actionTimes=0, windowEnd=0, rankName='0'}
//TopProductEntity{productId=14, actionTimes=0, windowEnd=0, rankName='0'}
//TopProductEntity{productId=13, actionTimes=0, windowEnd=0, rankName='1'}
//TopProductEntity{productId=13, actionTimes=0, windowEnd=0, rankName='1'}
//-------------Top N Product------------
//TopProductEntity{productId=14, actionTimes=0, windowEnd=0, rankName='0'}
//TopProductEntity{productId=14, actionTimes=0, windowEnd=0, rankName='0'}