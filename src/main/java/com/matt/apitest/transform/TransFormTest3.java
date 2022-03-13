package com.matt.apitest.transform;

import com.matt.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author matt
 * @create 2022-01-17 22:48
 */
public class TransFormTest3 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(16);
        DataStream<String> inputStream = env.readTextFile("/Users/matt/workspace/java/bigdata/study-flink/src/main/resources/sensor.txt");

        DataStream<SensorReading> dataStream = inputStream.map(s -> {
            String[] fields = s.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //分组
        KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");

        DataStream<SensorReading> resultStream = keyedStream.reduce(new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading v1, SensorReading v2) throws Exception {
                return new SensorReading(v1.getId(), v2.getTimestamp(),
                        Math.max(v1.getTemperatrue(), v2.getTemperatrue()));
            }
        });
        resultStream.print();
        // job name
        env.execute("trans-form");

    }

}
