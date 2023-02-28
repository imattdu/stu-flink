package com.matt.apitest.processfunction;

import com.matt.apitest.beans.Event;
import com.matt.apitest.source.SourceTest4_UDF;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

public class ProcessingTimeTimerTest1 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = env.addSource(new SourceTest4_UDF.ParallelCustomSource());


        stream.keyBy(d -> d.user)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    @Override
                    public void processElement(Event event, KeyedProcessFunction<String, Event, String>.Context context, Collector<String> collector) throws Exception {
                        long pTs = context.timerService().currentProcessingTime();
                        collector.collect(context.getCurrentKey() + "数据到达时间".concat(String.valueOf(pTs)));

                        // 注册10s 定时器
                        context.timerService().registerProcessingTimeTimer(pTs + 10 * 1000);
                    }

                    @Override
                    public void onTimer(long ts, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(ctx.getCurrentKey() + "定时器触发了， 触发时间" + new Timestamp(ts));
                    }
                })
                .print();

        env.execute();


    }
}
