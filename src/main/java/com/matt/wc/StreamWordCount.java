package com.matt.wc;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author matt
 * @create 2022-01-06 0:45
 */
// 流处理
public class StreamWordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 默认是cpu核数
        // env.setParallelism(16);
        // 从文件中读取数据
        //String inputPath = "D:\\matt\\workspace\\idea\\hadoop\\studyflink\\src\\main\\resources\\hello.txt";
        //DataStream<String> inputDataStream = env.readTextFile(inputPath);

        // nc 输入
        // parameter tool
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        DataStream<String> inputDataStream = env.socketTextStream(host, port);


        //基于数据流进行转换计算
        DataStream<Tuple2<String, Integer>> resultStream = inputDataStream.flatMap(new WordCount.MyFlatMapper())
                .keyBy(0)
                .sum(1).setParallelism(2).startNewChain();
        // .disableChaining();
        // 和前后都不合并任务

        // .startNewChain()
        // 开始一个新的任务链合并 前面断开 后面不断开
        resultStream.print().setParallelism(1);

        // 执行任务
        env.execute();

    }
}
