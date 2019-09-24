//package com.key2wen.hive;
//
//import org.apache.flink.api.common.functions.FilterFunction;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.api.java.DataSet;
//import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.api.java.operators.DataSource;
//import org.apache.flink.api.java.typeutils.RowTypeInfo;
//import org.apache.flink.types.Row;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.text.SimpleDateFormat;
//import java.util.Calendar;
//import java.util.Date;
//
//public class Test1 {
//
//    private final static String driverName = "org.apache.hive.jdbc.HiveDriver";// jdbc驱动路径
//    private final static String url = ";";// hive库地址+库名
//    private final static String user = "";// 用户名
//    private final static String password = "!";// 密码
//    private final static String table = "";
//    private final static String sql = " ";
//
//    static Logger logger = LoggerFactory.getLogger(Test1.class);
//
//    public static void main(String[] arg) throws Exception {
//
//        long time = System.currentTimeMillis();
//
//        logger.info("开始同步hive-" + table + ";");
//
//        /**
//         * 初始化环境
//         */
//        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(4);
//
//        try {
//            TypeInformation[] types = new TypeInformation[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO};
//            String[] colName = new String[]{"user", "name"};
//            RowTypeInfo rowTypeInfo = new RowTypeInfo(types, colName);
//
//            JDBCInputFormatBuilder builder = JDBCInputFormat.buildJDBCInputFormat().setDrivername(driverName)
//                    .setDBUrl(url)
//                    .setUsername(user).setPassword(password);
//
//            Calendar calendar = Calendar.getInstance();
//            calendar.setTime(new Date());
//            calendar.add(Calendar.DATE, -1); //用昨天产出的数据
//            SimpleDateFormat sj = new SimpleDateFormat("yyyyMMdd");
//            String d = sj.format(calendar.getTime());
//
//            JDBCInputFormat jdbcInputFormat = builder.setQuery(sql + " and dt='" + d + "' limit 100000000").setRowTypeInfo(rowTypeInfo).finish();
//            DataSource<Row> rowList = env.createInput(jdbcInputFormat);
//
//            DataSet<RedisDataModel> temp = rowList.filter(new FilterFunction<Row>() {
//
//                @Override
//                public boolean filter(Row row) throws Exception {
//                    String key = row.getField(0).toString();
//                    String value = row.getField(1).toString();
//                    if (key.length() < 5 || key.startsWith("-") || key.startsWith("$") || value.length() < 5 || value.startsWith("-") || value.startsWith("$")) {
//                        return false;
//                    } else {
//                        return true;
//                    }
//                }
//
//            }).map(new MapFunction<Row, RedisDataModel>() {
//
//                @Override
//                public RedisDataModel map(Row value) throws Exception {
//                    RedisDataModel m = new RedisDataModel();
//                    m.setExpire(-1);
//                    m.setKey("JobConstants.REDIS_FLINK_IMEI_USER" + value.getField(0).toString());
//                    m.setGlobal(true);
//                    m.setValue(value.getField(1).toString());
//                    return m;
//                }
//
//            });
//
//            logger.info("同步hive-" + table + "完成;开始推送模型,共有" + temp.count() + "条;");
//
//            RedisOutputFormat redisOutput = RedisOutputFormat.buildRedisOutputFormat()
//                    .setHostMaster(AppConfig.getProperty(JobConstants.REDIS_HOST_MASTER))
//                    .setHostSentinel(AppConfig.getProperty(JobConstants.REDIS_HOST_SENTINELS))
//                    .setMaxIdle(Integer.parseInt(AppConfig.getProperty(JobConstants.REDIS_MAXIDLE)))
//                    .setMaxTotal(Integer.parseInt(AppConfig.getProperty(JobConstants.REDIS_MAXTOTAL)))
//                    .setMaxWaitMillis(Integer.parseInt(AppConfig.getProperty(JobConstants.REDIS_MAXWAITMILLIS)))
//                    .setTestOnBorrow(Boolean.parseBoolean(AppConfig.getProperty(JobConstants.REDIS_TESTONBORROW)))
//                    .finish();
//
//            temp.output(redisOutput);
//            env.execute("hive-" + table + " sync");
//
//            logger.info("同步hive-" + table + "完成，耗时:" + (System.currentTimeMillis() - time) / 1000 + "s");
//        } catch (Exception e) {
//            logger.error("", e);
//            logger.info("同步hive-" + table + "失败,时间戳:" + time + ",原因：" + e.toString());
//        }
//    }
//
//}