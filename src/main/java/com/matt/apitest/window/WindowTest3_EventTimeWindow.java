package com.matt.apitest.window;

import com.matt.apitest.beans.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author matt
 * @create 2022-02-08 23:49
 */
public class WindowTest3_EventTimeWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);
        // 默认使用processTime
        // 从调用时刻开始给 env 创建的每一个 stream 追加时间特征
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(100);

        DataStream<String> inputStream = env.socketTextStream("localhost", 778);
        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        })
        //乱序
        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(SensorReading sensorReading) {
                // 必须为ms毫秒
                return sensorReading.getTimestamp() * 1000L;
            }
        });
        // 非乱序
        //.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SensorReading>() {
        //    @Override
        //    public long extractAscendingTimestamp(SensorReading sensorReading) {
        //        return sensorReading.getTimestamp() * 1000L;
        //    }
        //});

        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late"){};
        // 开窗测试
        // windowAll 都放在一个窗口里面
        SingleOutputStreamOperator<SensorReading> minTempStream = dataStream.keyBy("id")
                .timeWindow(Time.seconds(15))
               .allowedLateness(Time.minutes(1))
                // 1分钟后测输入流
               .sideOutputLateData(outputTag)
                .minBy("temperatrue");
        //temperatrue
        minTempStream.print("minTemp");

        minTempStream.getSideOutput(outputTag).print("late");



        //OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late") {
        //};
        //
        //// 基于事件时间的开窗聚合，统计15秒内温度的最小值
        //SingleOutputStreamOperator<SensorReading> minTempStream = dataStream.keyBy("id")
        //        .timeWindow(Time.seconds(15))
        //        .allowedLateness(Time.minutes(1))
        //        .sideOutputLateData(outputTag)
        //        .minBy("temperatrue");
        //
        //minTempStream.print("minTemp");
        //minTempStream.getSideOutput(outputTag).print("late");
        env.execute("my");

    }

}
