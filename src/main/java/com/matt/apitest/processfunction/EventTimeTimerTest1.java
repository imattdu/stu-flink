package com.matt.apitest.processfunction;

import com.matt.apitest.beans.Event;
import com.matt.apitest.source.SourceTest4_UDF;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import javax.transaction.TransactionRequiredException;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.concurrent.TransferQueue;

/**
 * @author matt
 * @create 2023-02-28 23:54
 * @desc xxx
 */
public class EventTimeTimerTest1 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = env.addSource(new CustomSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));

        stream.keyBy(d -> d.user)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    @Override
                    public void processElement(Event event, KeyedProcessFunction<String, Event, String>.Context context, Collector<String> collector) throws Exception {
                        long ts = context.timestamp();
                        String outD = context.getCurrentKey() + new Timestamp(ts);
                        outD += "wk" + context.timerService().currentWatermark();
                        collector.collect(outD);

                        // 注册10s 定时器
                        context.timerService().registerEventTimeTimer(ts + 10 * 1000);
                    }

                    @Override
                    public void onTimer(long ts, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(ctx.getCurrentKey() + "定时器触发了， 触发时间" + new Timestamp(ts) + "wk"
                                + ctx.timerService().currentWatermark());
                    }
                })
                .print();

        env.execute();
    }

    public static class CustomSource implements SourceFunction<Event> {

        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            ctx.collect(new Event("d1", "/detail", 1000L));
            Thread.sleep(5000L);

            ctx.collect(new Event("d2", "/detail", 11000L));
            Thread.sleep(5000L);

            ctx.collect(new Event("d2", "/detail", 11001L));
            Thread.sleep(5000L);
        }

        @Override
        public void cancel() {
        }
    }
}
