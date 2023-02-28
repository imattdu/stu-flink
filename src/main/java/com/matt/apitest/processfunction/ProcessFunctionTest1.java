package com.matt.apitest.processfunction;

import com.matt.apitest.beans.Event;
import com.matt.apitest.source.SourceTest4_UDF;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class ProcessFunctionTest1 {

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

        stream.process(new ProcessFunction<Event, String>() {
            @Override
            public void processElement(Event event, ProcessFunction<Event, String>.Context context, Collector<String> collector) throws Exception {
                if (event.user.equals("matt")) {
                    collector.collect("xx");
                }

                System.out.println(context.timestamp());
                long pTs = context.timerService().currentProcessingTime();
                long eTs = context.timerService().currentWatermark();
                System.out.println("pTs" + pTs + "eTs" + eTs);
            }
        }).print();

        env.execute();
    }
}
