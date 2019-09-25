package com.key2wen.state.exactlyonce;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 * User: zzy
 * Date: 2019/5/28
 * Time: 8:47 PM
 * To change this template use File | Settings | File Templates.
 * <p>
 * 自定义kafka to mysql,继承TwoPhaseCommitSinkFunction,实现两阶段提交
 * <p>
 * <p>
 * 最近项目中使用Flink消费kafka消息，并将消费的消息存储到mysql中，看似一个很简单的需求，
 * 在网上也有很多flink消费kafka的例子，但看了一圈也没看到能解决重复消费的问题的文章，
 * 于是在flink官网中搜索此类场景的处理方式，发现官网也没有实现flink到mysql的Exactly-Once例子，
 * 但是官网却有类似的例子来解决端到端的仅一次消费问题。这个现成的例子就是FlinkKafkaProducer011这个类，
 * 它保证了通过FlinkKafkaProducer011发送到kafka的消息是Exactly-Once的，主要的实现方式就是继承了TwoPhaseCommitSinkFunction这个类，
 * 关于TwoPhaseCommitSinkFunction这个类的作用可以先看上一篇文章：https://blog.51cto.com/simplelife/2401411。
 * <p>
 * <p>
 * 这里简单说下这个类的作用就是实现这个类的方法：beginTransaction、preCommit、commit、abort，
 * 达到事件（preCommit）预提交的逻辑（当事件进行自己的逻辑处理后进行预提交，
 * 如果预提交成功之后才进行真正的（commit）提交，如果预提交失败则调用abort方法进行事件的回滚操作），
 * 结合flink的checkpoint机制，来保存topic中partition的offset。
 * <p>
 * <p>
 * 达到的效果我举个例子来说明下：比如checkpoint每10s进行一次，此时用FlinkKafkaConsumer011实时消费kafka中的消息，
 * 消费并处理完消息后，进行一次预提交数据库的操作，如果预提交没有问题，10s后进行真正的插入数据库操作，如果插入成功，
 * 进行一次checkpoint，flink会自动记录消费的offset，可以将checkpoint保存的数据放到hdfs中，如果预提交出错，
 * 比如在5s的时候出错了，此时Flink程序就会进入不断的重启中，重启的策略可以在配置中设置，当然下一次的checkpoint也不会做了，
 * checkpoint记录的还是上一次成功消费的offset，本次消费的数据因为在checkpoint期间，消费成功，但是预提交过程中失败了，
 * 注意此时数据并没有真正的执行插入操作，因为预提交（preCommit）失败，提交（commit）过程也不会发生了。
 * 等你将异常数据处理完成之后，再重新启动这个Flink程序，它会自动从上一次成功的checkpoint中继续消费数据，
 * 以此来达到Kafka到Mysql的Exactly-Once。
 */
public class MySqlTwoPhaseCommitSink extends TwoPhaseCommitSinkFunction<ObjectNode, Connection, Void> {

    private static final Logger log = LoggerFactory.getLogger(MySqlTwoPhaseCommitSink.class);

    public MySqlTwoPhaseCommitSink() {
        super(new KryoSerializer<>(Connection.class, new ExecutionConfig()), VoidSerializer.INSTANCE);
    }

    /**
     * 执行数据库入库操作  task初始化的时候调用
     *
     * @param connection
     * @param objectNode
     * @param context
     * @throws Exception
     */
    @Override
    protected void invoke(Connection connection, ObjectNode objectNode, Context context) throws Exception {
        log.info("start invoke...");
        String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        log.info("===>date:" + date + " " + objectNode);
        log.info("===>date:{} --{}", date, objectNode);
        String value = objectNode.get("value").toString();
        log.info("objectNode-value:" + value);
        JSONObject valueJson = JSONObject.parseObject(value);
        String value_str = (String) valueJson.get("value");
        String sql = "insert into `mysqlExactlyOnce_test` (`value`,`insert_time`) values (?,?)";
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.setString(1, value_str);
        Timestamp value_time = new Timestamp(System.currentTimeMillis());
        ps.setTimestamp(2, value_time);
        log.info("要插入的数据:{}--{}", value_str, value_time);
        //执行insert语句
        ps.execute();
        //手动制造异常
        if (Integer.parseInt(value_str) == 15) {
            System.out.println(1 / 0);
        }
    }

    /**
     * 获取连接，开启手动提交事物（getConnection方法中）
     *
     * @return
     * @throws Exception
     */
    @Override
    protected Connection beginTransaction() throws Exception {
        log.info("start beginTransaction.......");
        String url = "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=false&autoReconnect=true";
        Connection connection = DBConnectUtil.getConnection(url, "root", "123456");
        return connection;
    }

    /**
     * 预提交，这里预提交的逻辑在invoke方法中
     *
     * @param connection
     * @throws Exception
     */
    @Override
    protected void preCommit(Connection connection) throws Exception {
        log.info("start preCommit...");
    }

    /**
     * 如果invoke方法执行正常，则提交事务
     *
     * @param connection
     */
    @Override
    protected void commit(Connection connection) {
        log.info("start commit...");
        DBConnectUtil.commit(connection);
    }

    /**
     * 如果invoke执行异常则回滚事物，下一次的checkpoint操作也不会执行
     *
     * @param connection
     */
    @Override
    protected void abort(Connection connection) {
        log.info("start abort rollback...");
        DBConnectUtil.rollback(connection);
    }
}