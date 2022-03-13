package com.matt.apitest.transform;

import com.matt.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import scala.Enumeration;

import java.util.Collections;

/**
 * @author matt
 * @create 2022-01-24 23:55
 */
public class TransfromTest5_RichFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        DataStream<String> inputStream = env.readTextFile("/Users/matt/workspace/java/bigdata/study-flink/src/main/resources/sensor.txt");

        DataStream<SensorReading> dataStream = inputStream.map(s -> {
            String[] fields = s.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });



        DataStream<Tuple2<String, Integer>> resStream = dataStream.map(
                new MyMapper()
        );
        resStream.print();


        // job name
        env.execute("trans-form");

    }


    public static class MyMapper extends RichMapFunction<SensorReading, Tuple2<String, Integer>> {


        @Override
        public Tuple2<String, Integer> map(SensorReading v) throws Exception {
            return new Tuple2<>(v.getId(), getRuntimeContext().getIndexOfThisSubtask());
        }

        public MyMapper() {
            super();
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("init....");
        }

        @Override
        public void close() throws Exception {
            System.out.println("clear...");
        }
    }

}
