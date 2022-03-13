package com.matt.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author matt
 * @create 2022-01-05 23:40
 */
// 批处理 离线数据集合
public class WordCount {


    public static void main(String[] args) throws Exception {
        // 1 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2 从文件中读取数据
        String inputPath = "/Users/matt/workspace/java/bigdata/study-flink/src/main/resources/hello.txt";
        DataSet<String> inputDataSet = env.readTextFile(inputPath);

        // 3 对数据集处理 按照空格分词展开 -》 （word, 1）
        // flatMap map 操作
        DataSet<Tuple2<String, Integer>> resSet = inputDataSet.flatMap(new MyFlatMapper())
                // 按照第一个位置分组
                .groupBy(0)
                // 将第二个位置上的数据求和
                .sum(1);

        // 4.输出
        resSet.print();

    }

    // 输入 输出
    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {

            // 分词
            String[] words = value.split(" ");
            // 加入到
            for (String word : words) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }



}
