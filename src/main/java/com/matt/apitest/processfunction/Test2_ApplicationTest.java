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

public class Test2_ApplicationTest {

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
                .process(new MyProcess1()).print();



        env.execute("my");

    }

    // 检测一段时间温度上升
    public static class MyProcess1 extends KeyedProcessFunction<Tuple, SensorReading, String> {

        // 时间间隔
        private Integer interval = 10;
        ValueState<Double> lastTempState;
        ValueState<Long> timerState;


        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("lastTempState", Double.class, Double.MIN_VALUE));
            timerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerState", Long.class));


        }


        @Override
        public void processElement(SensorReading sensorReading, KeyedProcessFunction<Tuple, SensorReading, String>.Context context, Collector<String> collector) throws Exception {
            Double lastTemp = lastTempState.value();
            Long lastTimer = timerState.value();

            // 状态是否存在
            if (sensorReading.getTemperatrue() > lastTemp && lastTimer == null) {
                long ts = context.timerService().currentProcessingTime() + interval * 1000l;
                // 注册一个时间
                context.timerService().registerProcessingTimeTimer(ts);
                timerState.update(ts);
            } else if (sensorReading.getTemperatrue() < lastTemp && lastTimer != null) {
                context.timerService().deleteEventTimeTimer(timerState.value());
                timerState.clear();
            }
            lastTempState.update(sensorReading.getTemperatrue());
        }


        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Tuple, SensorReading, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            System.out.println("持续上升" + ctx.getCurrentKey().getField(0));
            lastTempState.clear();
        }
    }


}
