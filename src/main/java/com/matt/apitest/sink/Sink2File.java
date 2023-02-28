package com.matt.apitest.sink;

import com.matt.apitest.beans.Event;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;


public class Sink2File {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<Event> stream = env.fromElements(new Event("a1", "/1", 1L),
                new Event("a1", "/2", 2L),
                new Event("a2", "/3", 3L),
                new Event("a2", "/2", 3L),
                new Event("a3", "/1", 3L),
                new Event("a2", "/3", 3L),
                new Event("a3", "/1", 3L),
                new Event("a2", "/2", 2L));

        StreamingFileSink<String> streamingFileSink = StreamingFileSink.<String>forRowFormat(new Path("./output"),
                new SimpleStringEncoder<>("UTF-8"))
                // 滚动策略
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        .withMaxPartSize(1024 * 1024 * 1024)
                        // ms
                        .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                        // 没有数据来
                        .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                        .build())
                .build();
        stream.map(data -> data.toString()).addSink(streamingFileSink);

        env.execute();
    }
}
