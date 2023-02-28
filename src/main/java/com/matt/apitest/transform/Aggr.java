package com.matt.apitest.transform;

import com.matt.apitest.beans.Event;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author matt
 * @create 2022-01-17 22:14
 */
public class Aggr {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(16);

        DataStream<Event> stream = env.fromElements(new Event("a1", "/1", 1L),
                new Event("a1", "/2", 2L),
                new Event("a2", "/1", 3L),
                new Event("a2", "/2", 2L));


        KeyedStream<Event, String> keyedSteam = stream.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event event) throws Exception {
                return event.user;
            }
        });

        keyedSteam.max("timestamp").print("1");

        // max 只更新统计的字段 maxBy 所有字段均更新
        stream.keyBy(e -> e.user).maxBy("timestamp")
                        .print("maxBy");

        env.execute();
    }

}
