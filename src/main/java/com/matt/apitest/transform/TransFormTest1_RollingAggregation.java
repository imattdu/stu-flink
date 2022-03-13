package com.matt.apitest.transform;

import com.matt.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author matt
 * @create 2022-01-17 22:14
 */
public class TransFormTest1_RollingAggregation {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(16);
        DataStream<String> inputStream = env.readTextFile("/Users/matt/workspace/java/bigdata/study-flink/src/main/resources/sensor.txt");
        
        // SensorReading
        //DataStream<SensorReading> dataStream = inputStream.map(new MapFunction<String, SensorReading>() {
        //
        //    @Override
        //    public SensorReading map(String value) throws Exception {
        //        String[] fields = value.split(",");
        //        return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        //    }
        //});

        DataStream<SensorReading> dataStream = inputStream.map( s -> {
            String[] fields = s.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

         //分组
        KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");

        // KeyedStream<SensorReading, String> keyedStream1 = dataStream.keyBy(d -> d.getId());
        // 滚动聚合
        // max 当前的字段 maxBy

        // maxBy 最大的那条记录 没有她大 非max字段也要改
        // max 最大值那条字段
        DataStream<SensorReading> resultStream = keyedStream.maxBy("temperatrue");
        resultStream.print();
        // keyedStream.print("keyed");
        // job name
        env.execute("trans-form");

    }

}
