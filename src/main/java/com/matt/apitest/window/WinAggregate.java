package com.matt.apitest.window;

import com.matt.apitest.beans.Event;
import com.matt.apitest.source.SourceTest4_UDF;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Timestamp;
import java.time.Duration;

public class WinAggregate {

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

        //
        dataStream.keyBy(d -> d.user)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new AggregateFunction<Event, Tuple2<Long, Integer>, String>() {

                    @Override
                    public Tuple2<Long, Integer> createAccumulator() {
                        return Tuple2.of(0L, 0);
                    }

                    // 叠加
                    @Override
                    public Tuple2<Long, Integer> add(Event event, Tuple2<Long, Integer> acc) {
                        return Tuple2.of(acc.f0 + event.timestamp, acc.f1 + 1);
                    }

                    @Override
                    public String getResult(Tuple2<Long, Integer> acc) {
                        Timestamp timestamp = new Timestamp(acc.f0 / acc.f1);
                        return timestamp.toString();
                    }

                    // 合并俩个累加器 会话窗口使用
                    @Override
                    public Tuple2<Long, Integer> merge(Tuple2<Long, Integer> a, Tuple2<Long, Integer> b) {
                        return Tuple2.of((a.f0 + b.f0), (a.f1 + b.f1));
                    }
                }).print();

        env.execute("matt");
    }
}
