package com.key2wen.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ExternalCatalog;
import org.apache.flink.table.catalog.InMemoryExternalCatalog;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;

public class Test1 {

    public static void main1(String[] args) throws Exception {
        // for batch programs use ExecutionEnvironment instead of StreamExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // create a TableEnvironment
        // for batch programs use BatchTableEnvironment instead of StreamTableEnvironment
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        // ***********
        // BATCH QUERY
        //// ***********
        ExecutionEnvironment bEnv = ExecutionEnvironment.getExecutionEnvironment();
        //// create a TableEnvironment for batch queries
        BatchTableEnvironment bTableEnv = TableEnvironment.getTableEnvironment(bEnv);


        //注意：注册Table的处理方式VIEW与关系数据库系统中已知的类似，即定义的查询Table未经优化，但在另一个查询引用已注册的内容时将内联Table。如果多个查询引用相同的注册Table，这将被内联的每个引用的查询和执行多次，即，注册的结果Table将不被共享。
        // Table is the result of a simple projection query
        Table projTable = tableEnv.scan("X").select("");

        //A TableSource提供对外部数据的访问，存储在存储系统中，例如数据库（MySQL，HBase，...），具有特定编码的文件（CSV，Apache [Parquet，Avro，ORC] ......）或消息系统（Apache Kafka，RabbitMQ，......）。
        TableSource csvSource = CsvTableSource.builder().path("")
                .build();


        //外部目录可以提供有关外部数据库和表的信息，例如其名称，架构，统计信息以及有关如何访问存储在外部数据库，表或文件中的数据的信息。
        ExternalCatalog catalog = new InMemoryExternalCatalog("");

        TableSink csvSink = new CsvTableSink("/path/to/file", "|");


        // register a Table
        tableEnv.registerTable("table1", projTable);            // or
        tableEnv.registerTableSource("table2", csvSource);     // or
        tableEnv.registerExternalCatalog("extCat", catalog);

        // create a Table from a  Table API query
        Table tapiResult = tableEnv.scan("table1").select("");
        // create a Table from a SQL query
        Table sqlResult = tableEnv.sqlQuery("SELECT ... FROM table2 ... ");
        // emit a  Table API result Table to a TableSink, same for SQL result

        tapiResult.writeToSink(csvSink);


        //已注册TableSink可用于将 Table API或SQL查询的结果发送到外部存储系统，例如数据库，键值存储，消息队列或文件系统（在不同的编码中，例如，CSV，Apache [Parquet] ，Avro，ORC]，......）。
        // define the field names and types
        String[] fieldNames = {"a", "b", "c"};
//        TypeInformation[] fieldTypes = {Types.INT, Types.STRING, Types.LONG};
        TypeInformation[] fieldTypes = {};
        // register the TableSink as table "CsvSinkTable"
        tableEnv.registerTableSink("CsvSinkTable", fieldNames, fieldTypes, csvSink);


        //todo 将DataStream或DataSet注册为表
        //A DataStream或DataSet可以在TableEnvironment表中注册。结果表的模式取决于已注册DataStream或的数据类型DataSet。有关详细信息，请查看有关将数据类型映射到表架构的部分。
        DataStream<Tuple2<Long, String>> stream = null;

        // register the DataStream as Table "myTable" with fields "f0", "f1"
        tableEnv.registerDataStream("myTable", stream);

        // register the DataStream as table "myTable2" with fields "myLong", "myString"
        tableEnv.registerDataStream("myTable2", stream, "myLong, myString");


        // register the DataStream as Table "myTable" with fields "f0", "f1"
        tableEnv.registerDataStreamInternal("myTable", stream);

        // register the DataStream as table "myTable2" with fields "myLong", "myString"
        Expression[] expressions = new Expression[2];
        tableEnv.registerDataStreamInternal("myTable2", stream, expressions);


        //todo 将DataStream或DataSet转换为表
        //它也可以直接转换为a 而不是注册a DataStream或DataSetin 。如果要在 Table API查询中使用Table，这很方便。TableEnvironmentTable
        // Convert the DataStream into a Table with default fields "f0", "f1"
        Table table1 = tableEnv.fromDataStream(stream);

        // Convert the DataStream into a Table with fields "myLong", "myString"
        Table table2 = tableEnv.fromDataStream(stream, "myLong, myString");

        //todo 原子类型
        //Flink认为原语（Integer，Double，String）或通用类型作为原子类型（无法进行分析和分解类型）。A DataStream或DataSet原子类型转换为Table具有单个属性的a。从原子类型推断属性的类型，并且可以指定属性的名称。
        // convert DataStream into Table with field "f1" only
        Table table3 = tableEnv.fromDataStream(stream, "f1");

        // convert DataStream into Table with swapped fields
        Table table4 = tableEnv.fromDataStream(stream, "f1, f0");

        // convert DataStream into Table with swapped fields and field names "myInt" and "myLong"
        Table table5 = tableEnv.fromDataStream(stream, "f1 as myInt, f0 as myLong");


        //todo 将表转换为DataStream或DataSet

        // convert the Table into an append DataStream of Row by specifying the class
        DataStream<Row> dsRow = tableEnv.toAppendStream(table1, Row.class);

        // convert the Table into an append DataStream of Tuple2<String, Integer>
        //   via a TypeInformation
        TupleTypeInfo<Tuple2<String, Integer>> tupleType = new TupleTypeInfo<>(Types.STRING(), Types.INT());
        DataStream<Tuple2<String, Integer>> dsTuple =
                tableEnv.toAppendStream(table1, tupleType);

        // convert the Table into a retract DataStream of Row.
        //   A retract stream of type X is a DataStream<Tuple2<Boolean, X>>.
        //   The boolean field indicates the type of the change.
        //   True is INSERT, false is DELETE.
        DataStream<Tuple2<Boolean, Row>> retractStream =
                tableEnv.toRetractStream(table1, Row.class);


        //todo 将表转换为DataSet
        // convert the Table into a DataSet of Row by specifying a class
        DataSet<Row> dsetRow = bTableEnv.toDataSet(table1, Row.class);

        // convert the Table into a DataSet of Tuple2<String, Integer> via a TypeInformation
        TupleTypeInfo<Tuple2<String, Integer>> tupleType2 = new TupleTypeInfo<>(
                Types.STRING(),
                Types.INT());
        DataSet<Tuple2<String, Integer>> dsTuple2 =
                bTableEnv.toDataSet(table1, tupleType);

        // execute
        env.execute();
    }

    //todo 以下示例显示了一个简单的 Table API聚合查询：
    public Table test1(StreamExecutionEnvironment env) {
        // get a StreamTableEnvironment, works for BatchTableEnvironment equivalently
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        // register Orders table

        // scan registered Orders table
        Table orders = tableEnv.scan("Orders");
        // compute revenue for all customers from France
        Table revenue = orders
                .filter("cCountry === 'FRANCE'")
                .groupBy("cID, cName")
                .select("cID, cName, revenue.sum AS revSum");

        return revenue;

        // emit or convert Table
        // execute query
    }

    //todo 以下示例显示如何指定查询并将结果作为a返回Table。
    public Table test2(StreamExecutionEnvironment env) {
        // get a StreamTableEnvironment, works for BatchTableEnvironment equivalently
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        // register Orders table

        // compute revenue for all customers from France
        Table revenue = tableEnv.sqlQuery(
                "SELECT cID, cName, SUM(revenue) AS revSum " +
                        "FROM Orders " +
                        "WHERE cCountry = 'FRANCE' " +
                        "GROUP BY cID, cName"
        );

        return revenue;

        // emit or convert Table
        // execute query
    }

    //todo 以下示例说明如何指定将其结果插入已注册表的更新查询。
    public void test3(StreamExecutionEnvironment env) {
        // get a StreamTableEnvironment, works for BatchTableEnvironment equivalently
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        // register "Orders" table
        // register "RevenueFrance" output table

        // compute revenue for all customers from France and emit to "RevenueFrance"
        tableEnv.sqlUpdate(
                "INSERT INTO RevenueFrance " +
                        "SELECT cID, cName, SUM(revenue) AS revSum " +
                        "FROM Orders " +
                        "WHERE cCountry = 'FRANCE' " +
                        "GROUP BY cID, cName"
        );

        // execute query
    }

    //一个Table由它写入发出TableSink。A TableSink是支持各种文件格式（例如CSV，Apache Parquet，Apache Avro），存储系统（例如，JDBC，Apache HBase，Apache Cassandra，Elasticsearch）或消息传递系统（例如，Apache Kafka，RabbitMQ）的通用接口）。
    //todo 以下示例显示如何发出Table：
    public void tset4(StreamExecutionEnvironment env) {
        // get a StreamTableEnvironment, works for BatchTableEnvironment equivalently
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        // compute a result Table using  Table API operators and/or SQL queries
        Table result = test1(env);

        // create a TableSink
        TableSink sink = new CsvTableSink("/path/to/file", "|");

        // METHOD 1:
        //   Emit the result Table to the TableSink via the writeToSink() method
        result.writeToSink(sink);

        // METHOD 2:
        //   Register the TableSink with a specific schema
        String[] fieldNames = {"a", "b", "c"};
//        TypeInformation[] fieldTypes = {Types.INT, Types.STRING, Types.LONG};
        TypeInformation[] fieldTypes = {};
        tableEnv.registerTableSink("CsvSinkTable", fieldNames, fieldTypes, sink);
        //   Emit the result Table to the registered TableSink via the insertInto() method
        result.insertInto("CsvSinkTable");

        // execute the program
    }

    /**
     * 上面的代码：
     * 翻译并执行查询
     * Table API和SQL查询将转换为DataStream或DataSet程序，具体取决于它们的输入是流式还是批量输入。查询在内部表示为逻辑查询计划，并分为两个阶段：
     * <p>
     * 优化逻辑计划，
     * 转换为DataStream或DataSet程序。
     * 在以下情况下转换 Table API或SQL查询：
     * <p>
     * a Table被发射到a TableSink，即何时Table.writeToSink()或被Table.insertInto()称为。
     * 指定了SQL更新查询，即TableEnvironment.sqlUpdate()调用时。
     * a Table转换为a DataStream或DataSet（请参阅与DataStream和DataSet API集成）。
     * <p>
     * 一旦翻译，一 Table API或SQL查询像一个普通的数据流中或数据集处理程序，当被执行StreamExecutionEnvironment.execute()或者ExecutionEnvironment.execute()被调用。
     */


    /**
     * 解释表
     * Table API提供了一种机制来解释计算a的逻辑和优化查询计划Table。这是通过该TableEnvironment.explain(table)方法完成的。它返回一个描述三个计划的String：
     * <p>
     * 关系查询的抽象语法树，即未优化的逻辑查询计划，
     * 优化的逻辑查询计划，以及
     * 物理执行计划。
     * 以下代码显示了一个示例和相应的输出：
     */

    public static void main(String[] a) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

        DataStream<Tuple2<Integer, String>> stream1 = env.fromElements(new Tuple2<>(1, "hello"));
        DataStream<Tuple2<Integer, String>> stream2 = env.fromElements(new Tuple2<>(1, "hello"));

        Table table1 = tEnv.fromDataStream(stream1, "count, word");
        Table table2 = tEnv.fromDataStream(stream2, "count, word");
        Table table = table1
                .where("LIKE(word, 'F%')")
                .unionAll(table2);

        String explanation = tEnv.explain(table);
        System.out.println(explanation);
    }
}
