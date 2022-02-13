package com.matt.apitest.window;

import com.matt.apitest.beans.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author matt
 * @create 2022-02-08 22:14
 */
public class WindowTest2_CountWindow {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> inputStream = env.socketTextStream("localhost", 777);
        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });
        // 开窗测试
        // windowAll 都放在一个窗口里面
        SingleOutputStreamOperator<Double> avgTmpeResultStream = dataStream.keyBy("id")
                // 滚动
                .countWindow(10, 2)
                .aggregate(new MyAvgTemp());


        // 全窗口函数
        avgTmpeResultStream.print();

        env.execute("my");

    }

    public static class MyAvgTemp implements AggregateFunction<SensorReading, Tuple2<Double, Integer>, Double> {

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0, 0);
        }

        @Override
        public Tuple2<Double, Integer> add(SensorReading v, Tuple2<Double, Integer> old) {

            return new Tuple2<>(old.f0 + v.getTemperatrue(), old.f1 + 1);
        }

        @Override
        public Double getResult(Tuple2<Double, Integer> old) {
            return old.f0 / old.f1;
        }

        @Override
        public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> a, Tuple2<Double, Integer> b) {
            return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
        }
    }

}
