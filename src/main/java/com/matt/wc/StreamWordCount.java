package com.matt.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 默认是cpu核数
        // env.setParallelism(16);
        // 从文件中读取数据

        // nc 输入
         ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");
        DataStream<String> inputDataStream = env.socketTextStream(host, port);

        // 基于数据流进行转换计算
        DataStream<Tuple2<String, Long>> resultStream = inputDataStream.flatMap(
                        (String line, Collector<Tuple2<String, Long>> out) -> {
                            String[] inArr = line.split(" ");
                            for (String i : inArr) {
                                out.collect(Tuple2.of(i, 1L));
                            }
                        }
                ).returns(Types.TUPLE(Types.STRING, Types.LONG))
                // 设置共享组 在同一个共享组才可以共享 默认都在 default 共享组
                .slotSharingGroup("1")
                .keyBy(data -> data.f0)
                .sum(1);

        resultStream.print();

        // 执行任务
        env.execute();
    }

}
