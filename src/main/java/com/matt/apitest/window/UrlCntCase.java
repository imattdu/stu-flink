package com.matt.apitest.window;

import com.matt.apitest.beans.Event;
import com.matt.apitest.model.UrlCntBO;
import com.matt.apitest.source.SourceTest4_UDF;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class UrlCntCase {

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


        dataStream.keyBy(d -> d.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new UrlCntAgg(), new UrlCntResult())
                .print();

        env.execute();
    }

    public static class UrlCntAgg implements AggregateFunction<Event, Long , Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event event, Long acc) {
            return acc + 1;
        }

        @Override
        public Long getResult(Long acc) {
            return acc;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return null;
        }
    }


    public static class UrlCntResult extends ProcessWindowFunction<Long, UrlCntBO, String, TimeWindow> {

        @Override
        public void process(String s, ProcessWindowFunction<Long, UrlCntBO, String, TimeWindow>.Context ctx, Iterable<Long> iterable, Collector<UrlCntBO> collector) throws Exception {
            Long start = ctx.window().getStart();
            Long end = ctx.window().getEnd();
            Long uv = iterable.iterator().next();
            collector.collect(new UrlCntBO(s, uv, start, end));
        }
    }

}
