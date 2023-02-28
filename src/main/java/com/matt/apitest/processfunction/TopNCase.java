package com.matt.apitest.processfunction;

import com.matt.apitest.beans.Event;
import com.matt.apitest.model.UrlCntBO;
import com.matt.apitest.source.SourceTest4_UDF;
import com.matt.apitest.window.UrlCntCase;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @author matt
 * @create 2023-03-01 00:35
 * @desc xxx
 */
public class TopNCase {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new SourceTest4_UDF.ParallelCustomSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));

        SingleOutputStreamOperator<UrlCntBO> urlCntStream = stream.keyBy(d -> d.url)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new UrlCntCase.UrlCntAgg(), new UrlCntCase.UrlCntResult());

        urlCntStream.print("urlCntStream");


        env.execute();
    }
}
