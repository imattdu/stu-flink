package com.matt.apitest.window;

import com.matt.apitest.beans.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingAlignedProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author matt
 * @create 2022-02-02 11:49
 */
public class WindowTest1_TimeWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);



        DataStream<String> inputStream = env.socketTextStream("localhost", 777);
        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });
        // 开窗测试
        // windowAll 都放在一个窗口里面
        DataStream<Integer> t1 = dataStream.keyBy("id")
                //.window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                .timeWindow(Time.seconds(1))
                //.window(EventTimeSessionWindows.withGap(Time.seconds(15)));
                // 1 滚动 2 滑动
                //.countWindow(15);
                // 中间聚合 输出
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {

                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(SensorReading sensorReading, Integer integer) {
                        return integer + 1;
                    }

                    @Override
                    public Integer getResult(Integer integer) {
                        return integer;
                    }

                    @Override
                    public Integer merge(Integer integer, Integer acc1) {
                        return null;
                    }
                });

        // t1.print();
        // job name
        DataStream<Tuple3<String, Long, Integer>> t2 = dataStream.keyBy("id")
                .timeWindow(Time.seconds(10))
                .apply(new WindowFunction<SensorReading, Tuple3<String, Long, Integer>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<SensorReading> input, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
                        String id = tuple.getField(0);
                        long windowEnd = timeWindow.getEnd();
                        int count = IteratorUtils.toArray(input.iterator()).length;
                        Tuple3<String, Long, Integer> res = new Tuple3<>(id, windowEnd, count);
                        out.collect(res);
                    }


                });

        // 全窗口函数
        t2.print();


        // 其他api
        OutputTag<SensorReading> outputTag = new OutputTag<>("rate");

        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> t3 = dataStream.keyBy("id")
                .timeWindow(Time.seconds(10))
                .allowedLateness(Time.minutes(5))
                .sideOutputLateData(outputTag)
                .apply(new WindowFunction<SensorReading, Tuple3<String, Long, Integer>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<SensorReading> input, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
                        String id = tuple.getField(0);
                        long windowEnd = timeWindow.getEnd();
                        int count = IteratorUtils.toArray(input.iterator()).length;
                        Tuple3<String, Long, Integer> res = new Tuple3<>(id, windowEnd, count);
                        out.collect(res);
                    }


                });


        t3.getSideOutput(outputTag).print("rate");
        
        env.execute("my");

    }


}
