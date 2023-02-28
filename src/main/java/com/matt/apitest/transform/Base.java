package com.matt.apitest.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author matt
 * @create 2022-01-16 17:16
 * map flatmap filter
 */
public class Base {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(16);
        DataStream<String> source = env.readTextFile("/Users/matt/workspace/java/stu/stu-flink/src/main/resources/sensor.txt");

        // 1 map string -> len(string) 1:1
        // 方-》园
        DataStream<Integer> mapStream = source.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String s) throws Exception {
                return s.length();
            }
        });


        DataStream<Integer> lMapStream = source.map(data -> data.length());

        // flatMap 按逗号切分字端 1:n
        DataStream<String> flatMapStream = source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] fields = s.split(",");
                for (String field : fields) {
                    collector.collect(field);
                }
            }
        });

        // 3.filter 过滤 筛选某个数据 1:[0,1]
        DataStream<String> filterStream = source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                // true 要 false 不要这个数据
                return s.startsWith("sensor_1");
            }
        });

        mapStream.print("map");
        flatMapStream.print("flatMap");
        filterStream.print("filter");

        // job name
        env.execute("trans-form");
    }


}
