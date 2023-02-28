package com.matt.apitest.transform;

import com.matt.apitest.beans.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class Partition {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(16);

        DataStream<Event> stream = env.fromElements(new Event("a1", "/1", 1L),
                new Event("a1", "/2", 2L),
                new Event("a2", "/3", 3L),
                new Event("a2", "/2", 3L),
                new Event("a3", "/1", 3L),
                new Event("a2", "/3", 3L),
                new Event("a3", "/1", 3L),
                new Event("a2", "/2", 2L));

        // 2.1 keyBy 逻辑分区 shuffle 随机分区
        //stream.shuffle().print().setParallelism(4);

        // 2.2轮询分区 2 3 1 4 # 2 3 1 4
        //stream.rebalance().print("rebalance").setParallelism(4);

        // 2.3 rescale 重缩放分区 分组，组内轮询 上游分区2 2 个分区内内部轮询
        DataStream<Integer> rescaleStream = env.addSource(new RichParallelSourceFunction<Integer>() {
            @Override
            public void run(SourceContext<Integer> ctx) throws Exception {
                for (int i = 1; i <= 8; i++) {
                    if (i % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
                        ctx.collect(i);
                    }
                }
            }

            @Override
            public void cancel() {

            }
        }).setParallelism(2);
        // 3,4 均是奇书 1,2 均是偶数
        //rescaleStream.rescale().print("rescale").setParallelism(4);

        // 2.4 广播 一份数据向每个分区都发送
        //stream.broadcast().print("broadcast").setParallelism(4);

        //2.5 全局分区 等价并行度为1
        //stream.global().print("global").setParallelism(4);

        // 2.6 自定义重分区
        env.fromElements(1, 2, 3, 4, 5).partitionCustom(new Partitioner<Integer>() {
            @Override
            public int partition(Integer k, int cnt) {
                return k % 2;
            }
        }, new KeySelector<Integer, Integer>() {
            @Override
            public Integer getKey(Integer v) throws Exception {
                return v;
            }
        }).print("custom").setParallelism(4);

        // job name
        env.execute("partition");
    }

}
