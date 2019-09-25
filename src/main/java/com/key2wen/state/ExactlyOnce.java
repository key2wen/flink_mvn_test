//package com.key2wen.state;
//
//import org.apache.flink.streaming.api.TimeCharacteristic;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
//import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;
//import org.apache.kafka.clients.producer.ProducerConfig;
//
//import java.util.Properties;
//
//public class ExactlyOnce {
//
//    public static void main(String[] args) throws Exception {
//
//        final int maxEventDelay = 60;       // events are out of order by max 60 seconds
//        final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second
//
//        // set up streaming execution environment
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//
//        String rideInput = "./taxi-data/nycTaxiRides.gz";
//        String taxiRideTopicId = "taxi-ride";
//
//        // start the data generator
//        DataStream<TaxiRide> rides = env.addSource(
//                new CheckpointedTaxiRideSource(rideInput, servingSpeedFactor));
//
//        Properties properties = new Properties();
//        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//
//        SerializationSchema<TaxiRide> taxiRideSerializationSchema = new TaxiRideSchema();
//
//        rides.addSink(new FlinkKafkaProducer011<TaxiRide>(taxiRideTopicId,
//                new KeyedSerializationSchemaWrapper(taxiRideSerializationSchema),
//                properties,
//                FlinkKafkaProducer011.Semantic.EXACTLY_ONCE // 开启Kafka EOS
//        ));
//
//        env.execute("send taxi ride to kafka ");
//    }
//}
