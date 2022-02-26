package com.matt.apitest.state;

import com.matt.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.Collections;
import java.util.List;

public class StateTest1 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> inputStream = env.socketTextStream("localhost", 777);
        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 有状态的map操作, 当前分区数据个数
        SingleOutputStreamOperator<Integer> count = dataStream.map(new MyCountMapper());

        count.print();

        env.execute("my");

    }


    public static class MyCountMapper implements MapFunction<SensorReading, Integer>, ListCheckpointed<Integer> {

        // 算子状态
        private Integer count = 0;

        @Override
        public Integer map(SensorReading sensorReading) throws Exception {
            count++;
            return count;
        }


        //
        @Override
        public List<Integer> snapshotState(long l, long l1) throws Exception {
            return Collections.singletonList(count);
        }

        //
        @Override
        public void restoreState(List<Integer> list) throws Exception {
            for (Integer num : list) {
                count += num;
            }
        }
    }

}
