package com.matt.apitest.state;

import com.matt.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StateTest3_keyedstate_app1 {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> inputStream = env.socketTextStream("localhost", 777);
        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // warn
        SingleOutputStreamOperator<Tuple3<String, Double, Double>> res = dataStream.keyBy("id")
                .flatMap(new TempChangeWarning(10.0));

        res.print();

        env.execute("my");

    }


    public static class TempChangeWarning extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>> {

        private Double threshold = 10.0;

        // 状态 上一次温度值
        private ValueState<Double> lastTempState;

        public TempChangeWarning(Double threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Double>("last-temp", Double.class)
            );
        }

        @Override
        public void flatMap(SensorReading sensorReading, Collector<Tuple3<String, Double, Double>> collector) throws Exception {

            Double lastTemp = lastTempState.value();

            if (lastTemp != null && Math.abs(sensorReading.getTemperatrue() - lastTemp) > threshold) {
                collector.collect(new Tuple3<>("报警" + sensorReading.getId(), sensorReading.getTemperatrue(), lastTemp));
            }

            lastTempState.update(sensorReading.getTemperatrue());
        }

        @Override
        public void close() throws Exception {
            // 清理状态
            lastTempState.clear();
        }
    }

}
