package com.matt.apitest.source;

import com.matt.apitest.beans.Event;
import com.matt.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author matt
 * @create 2022-01-16 14:36
 */
public class SourceTest1_Collection {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(5);
        DataStream<Event> events = env.fromCollection(Arrays.asList(
                new Event("m1", "a1", 1672472280000L),
                new Event("m2", "a2", 1672472279514L)
        ));

        events.print("collection");
        // job name
        env.execute("my");

    }
}
