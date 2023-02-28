package com.matt.apitest.window;

import com.matt.apitest.beans.Event;
import com.matt.apitest.source.SourceTest4_UDF;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashSet;
import java.util.Iterator;

public class WindowProcessTest1 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // >=1.12 不需要设置开启watermark
        // 100ms 触发一次水位线生成
        //env.getConfig().setAutoWatermarkInterval(100);
        DataStream<Event> dataStream = env.addSource(new SourceTest4_UDF.ParallelCustomSource())
                .setParallelism(1)
                // 乱序 延迟
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));


        dataStream.keyBy(data -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new UVCountByWindow())
                .print();

        env.execute("全窗口函数");
    }

    public static class UVCountByWindow extends ProcessWindowFunction<Event, String, Boolean, TimeWindow> {

        @Override
        public void process(Boolean key, ProcessWindowFunction<Event, String, Boolean, TimeWindow>.Context ctx, Iterable<Event> iterable, Collector<String> collector) throws Exception {
            HashSet<String> usernameSet = new HashSet<>();
            for (Event e : iterable) {
                usernameSet.add(e.user);
            }
            collector.collect(ctx.window().getStart() / 1000 + "-" + ctx.window().getEnd() / 1000 + "uv->" + usernameSet.size());
        }
    }
}
