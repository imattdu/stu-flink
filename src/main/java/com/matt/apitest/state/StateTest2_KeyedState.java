package com.matt.apitest.state;

import com.matt.apitest.beans.SensorReading;
import org.apache.commons.digester.SetNestedPropertiesRule;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.util.hash.Hash;
import sun.management.Sensor;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class StateTest2_KeyedState {


    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> inputStream = env.socketTextStream("localhost", 777);
        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        SingleOutputStreamOperator<Integer> res = dataStream.keyBy("id").map(new MyKeyCountMapper());

        res.print();

        env.execute("my");

    }


    public static class MyKeyCountMapper extends RichMapFunction<SensorReading, Integer>{

        private ValueState<Integer> keyCountState;

        private ListState<String> listState;
        private MapState<String, String> mapState;

        private ReducingState<SensorReading> readingReducingState;

        @Override
        public void open(Configuration parameters) throws Exception {
            keyCountState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Integer>("keyedCount", Integer.class,0)
            );

            // name:不能相同
            listState = getRuntimeContext().getListState(
                    new ListStateDescriptor<String>("listState", String.class)
            );

            mapState = getRuntimeContext().getMapState(
              new MapStateDescriptor<String, String>("mapState", String.class, String.class)
            );

            //readingReducingState = getRuntimeContext().getReducingState(
            //    new ReducingStateDescriptor<SensorReading>("reducingState", new )
            //);
        }

        @Override
        public Integer map(SensorReading sensorReading) throws Exception {
            Integer count = keyCountState.value();
            count++;
            keyCountState.update(count);

            Iterable<String> strings = listState.get();

            for (String string : strings) {
                System.out.println(string) ;
            }

            listState.add("hello");

            // mapState


            // readingReducingState add 方法直接聚合


            return count;
        }
    }

}
