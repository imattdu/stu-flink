package com.matt.apitest.transform;

import com.matt.apitest.beans.SensorReading;
import org.apache.commons.math3.geometry.partitioning.SubHyperplane;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Collections;

/**
 * @author matt
 * @create 2022-01-17 23:30
 */
public class TransFromTest4_MultiplStreams {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(16);
        DataStream<String> inputStream = env.readTextFile("/Users/matt/workspace/java/bigdata/study-flink/src/main/resources/sensor.txt");

        DataStream<SensorReading> dataStream = inputStream.map(s -> {
            String[] fields = s.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });


        //1. 分流
        SplitStream<SensorReading> splitStream = dataStream.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading sensorReading) {
                return (sensorReading.getTemperatrue() > 30) ? Collections.singletonList("high") :
                        Collections.singletonList("low") ;
            }
        });

        DataStream<SensorReading> highTempStream = splitStream.select("high");
        DataStream<SensorReading> lowTempStream = splitStream.select("low");




        highTempStream.print();
        System.out.println("-------");
        lowTempStream.print();


        System.out.println("河流");
        DataStream<Tuple2<String, Double>> warningStream = highTempStream.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(SensorReading v) throws Exception {
                return new Tuple2<>(v.getId(), v.getTemperatrue());
            }
        });

        ConnectedStreams<Tuple2<String, Double>, SensorReading> connectedStreams = warningStream.connect(lowTempStream);


        SingleOutputStreamOperator<Object> res = connectedStreams.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
            @Override
            public Object map1(Tuple2<String, Double> v) throws Exception {
                return new Tuple3<>(v.f0, v.f1, "high temp warning");
            }

            @Override
            public Object map2(SensorReading v) throws Exception {
                return new Tuple2<>(v.getId(), "normal");
            }
        });

        res.print();

        System.out.println("union");
        DataStream<SensorReading> union = highTempStream.union(lowTempStream);
        union.print();

        // job name
        env.execute("trans-form");

    }

}
