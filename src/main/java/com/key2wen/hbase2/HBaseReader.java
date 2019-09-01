package com.key2wen.hbase2;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * @Description hbase reader
 * @Author jiangxiaozhi
 * @Date 2018/10/17 10:05
 * <p>
 * 这里读HBase提供两种方式，一种是继承RichSourceFunction，重写父类方法，一种是实现OutputFormat接口，具体代码如下：
 * <p>
 * 方式一：继承RichSourceFunction
 *
 * 方式二：重写TableInputFormat方法 : HBaseReadMain
 **/
public class HBaseReader extends RichSourceFunction<Tuple2<String, String>> {
    private static final Logger logger = LoggerFactory.getLogger(HBaseReader.class);

    private Connection conn = null;
    private Table table = null;
    private Scan scan = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

//        conn = HBaseConnection.getHBaseConn();
        conn = ConnectionFactory.createConnection(null);


        table = conn.getTable(TableName.valueOf(""));
        scan = new Scan();
        scan.setStartRow(Bytes.toBytes("1001"));
        scan.setStopRow(Bytes.toBytes("1004"));
        scan.addFamily(Bytes.toBytes(""));

    }

    @Override
    public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
        ResultScanner rs = table.getScanner(scan);
        Iterator<Result> iterator = rs.iterator();
        while (iterator.hasNext()) {
            Result result = iterator.next();
            String rowkey = Bytes.toString(result.getRow());
            StringBuffer sb = new StringBuffer();
            for (Cell cell : result.listCells()) {
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                sb.append(value).append(",");
            }
            String valueString = sb.replace(sb.length() - 1, sb.length(), "").toString();
            Tuple2<String, String> tuple2 = new Tuple2<>();
            tuple2.setFields(rowkey, valueString);
            ctx.collect(tuple2);
        }

    }

    @Override
    public void cancel() {
        try {
            if (table != null) {
                table.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (IOException e) {
            logger.error("Close HBase Exception:", e.toString());
        }

    }
}
