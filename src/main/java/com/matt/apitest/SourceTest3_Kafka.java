package com.matt.apitest;

import com.matt.apitest.beans.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author matt
 * @create 2022-01-16 15:05
 */
public class SourceTest3_Kafka {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "matt05:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        DataStream<String> dataStream = env.addSource( new
                FlinkKafkaConsumer011<String>("sensor", new SimpleStringSchema(), properties));

        dataStream.print("kafka");

        // job name
        env.execute("kafak_job");


    }
}
