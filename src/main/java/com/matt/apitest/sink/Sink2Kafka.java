package com.matt.apitest.sink;

import com.matt.apitest.beans.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;


/**
 * @author matt
 * @create 2022-01-25 23:33
 */
public class Sink2Kafka {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        DataStream<String> kafkaStream = env.addSource( new
                FlinkKafkaConsumer<String>("first", new SimpleStringSchema(), properties));

        //
        SingleOutputStreamOperator<String> res = kafkaStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                String[] fileds = s.split(",");
                return new Event(fileds[0], fileds[1], Long.valueOf(fileds[2])).toString();
            }
        });

        res.addSink(new FlinkKafkaProducer<String>("localhost:9092", "test", new SimpleStringSchema()));
        // job name
        env.execute("SINK_KAFKA");
    }
}
