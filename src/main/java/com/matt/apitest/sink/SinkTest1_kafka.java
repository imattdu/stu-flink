package com.matt.apitest.sink;

import com.matt.apitest.beans.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.SimpleTimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

/**
 * @author matt
 * @create 2022-01-25 23:33
 */
public class SinkTest1_kafka {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);
        DataStream<String> dataStream = env.readTextFile("D:\\matt\\workspace\\idea\\hadoop\\study-flink\\src\\main\\resources\\sensor.txt");

        dataStream.print("file");

        DataStream<String> resStream = dataStream.map(line -> {
            String[] f = line.split(",");
            return new SensorReading(f[0], new Long(f[1]), new Double(f[2])).toString();
        });

        resStream.print();

        resStream.addSink(new FlinkKafkaProducer011<String>("matt05:9092",
                "test", new SimpleStringSchema()));


        // job name
        env.execute("my");

    }
}
