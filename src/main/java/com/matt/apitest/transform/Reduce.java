package com.matt.apitest.transform;

import com.matt.apitest.beans.Event;
import com.matt.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author matt
 * @create 2022-01-17 22:48
 * 高级聚合
 */
public class Reduce {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(16);

        DataStream<Event> stream = env.fromElements(new Event("a1", "/1", 1L),
                new Event("a1", "/2", 2L),
                new Event("a2", "/3", 3L),
                new Event("a2", "/2", 3L),
                new Event("a3", "/1", 3L),
                new Event("a2", "/3", 3L),
                new Event("a3", "/1", 3L),
                new Event("a2", "/2", 2L));

        // 2. 用户点击次数
        DataStream<Tuple2<String, Long>> userClickCntStream = stream.map(new MapFunction<Event, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(Event event) throws Exception {
                return Tuple2.of(event.user, 1L);
            }
        }).keyBy(d -> d.f0).reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> a, Tuple2<String, Long> b) throws Exception {

                return Tuple2.of(a.f0, a.f1 + b.f1);
            }
        });

        // 3.当前最活跃的用户
        userClickCntStream.keyBy(d -> "xx").reduce((x, y) ->
                x.f1 >= y.f1 ? x : y
        ).print("maxActiveUser");

        // job name
        env.execute("reduce");
    }
}

