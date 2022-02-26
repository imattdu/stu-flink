package com.matt.apitest.processfunction;

import com.matt.apitest.beans.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Test1_ProcessKeyedProcessFunction {


    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> inputStream = env.socketTextStream("localhost", 777);
        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });


        // 测试 keyedProcessFunction

        dataStream.keyBy("id")
                        .process(new MyProcess()).print();



        env.execute("my");

    }


    public static class MyProcess extends KeyedProcessFunction<Tuple, SensorReading, Integer> {

        ValueState<Long> tsTimerState;

        @Override
        public void open(Configuration parameters) throws Exception {
            tsTimerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-timer", Long.class));

        }

        @Override
        public void processElement(SensorReading sensorReading, KeyedProcessFunction<Tuple, SensorReading, Integer>.Context context, Collector<Integer> collector) throws Exception {
            collector.collect(sensorReading.getId().length());

            Long timestamp = context.timestamp();
            Tuple currentKey = context.getCurrentKey();
            // context.output();
            context.timerService().currentProcessingTime();
            context.timerService().currentWatermark();

            context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime() + 1000);

            tsTimerState.update(context.timerService().currentProcessingTime() + 1000);

            // ms
            context.timerService().registerEventTimeTimer((sensorReading.getTimestamp() + 10) * 1000);

            //context.timerService().deleteEventTimeTimer(tsTimerState.value());

        }


        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Tuple, SensorReading, Integer>.OnTimerContext ctx, Collector<Integer> out) throws Exception {
            System.out.println(timestamp + "定时器触发");

        }
    }



}
