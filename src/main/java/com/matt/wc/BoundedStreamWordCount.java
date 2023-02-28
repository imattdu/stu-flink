package com.matt.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author matt
 * @create 2022-01-06 0:45
 */
// 流处理
public class BoundedStreamWordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 默认是cpu核数
        // env.setParallelism(16);
        // 从文件中读取数据
        String inputPath = "/Users/matt/workspace/java/stu/stu-flink/src/main/resources/hello.txt";
        DataStream<String> inputDataStream = env.readTextFile(inputPath);

        // nc 输入
        // parameter tool
        // ParameterTool parameterTool = ParameterTool.fromArgs(args);
        //String host = parameterTool.get("host");
        //int port = parameterTool.getInt("port");
        //
        //DataStream<String> inputDataStream = env.socketTextStream(host, port);


        // 基于数据流进行转换计算
        DataStream<Tuple2<String, Long>> resultStream = inputDataStream.flatMap(
                        (String line, Collector<Tuple2<String, Long>> out) -> {
                            String[] inArr = line.split(" ");
                            for (String i : inArr) {
                                out.collect(Tuple2.of(i, 1L));
                            }
                        }
                ).returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(data -> data.f0)
                .sum(1); // .setParallelism(2).startNewChain();
        // 和前后都不合并任务
        // .disableChaining();


        // 开始一个新的任务链合并 前面断开 后面不断开
        // .startNewChain()

        resultStream.print(); //.setParallelism(1);

        // 执行任务
        env.execute();
    }
}
