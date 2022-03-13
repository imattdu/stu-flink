package com.matt.apitest.state;

import com.matt.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StateTest4_FaultTolerance {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStateBackend(new MemoryStateBackend());

        // 文件系统
        //new FsStateBackend()
        //
        // new RocksDBStateBackend("")


        // 检查点配置 ms
        env.enableCheckpointing(500);

        // 高级选项 EXACTLY_ONCE精确一次    xx 至少一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 超时时间 防止卡在一直保存
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        // 同时有2个检查点 ： 前一个没有保存完成 后一个保存开始 此时有2个保存
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);

        // 前一次保存结束 到下一次保存开始
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(100L);



        // 3.重启策略配置
        // fallback基于上层容器
        // fixedDelayRestart 固定延迟重启
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000));

        //failureRateRestart 重启次数 所有重启最多的总时间 重启间隔
        // env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.seconds(10), Time.seconds(2)));


        DataStream<String> inputStream = env.socketTextStream("localhost", 777);
        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });


        dataStream.print();

        env.execute("my");

    }

}
