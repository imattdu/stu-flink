package com.matt.apitest.window;

import com.matt.apitest.beans.Event;
import com.matt.apitest.model.UrlCntBO;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

// 延迟处理
public class LateDataTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // >=1.12 不需要设置开启watermark
        // 100ms 触发一次水位线生成
        //env.getConfig().setAutoWatermarkInterval(100);
        env.setParallelism(1);
        DataStream<Event> dataStream = env.socketTextStream("localhost", 777)
                .map(
                        new MapFunction<String, Event>() {
                            @Override
                            public Event map(String line) throws Exception {
                                String[] fArr = line.split(" ");
                                return new Event(fArr[0], fArr[1], Long.valueOf(fArr[2]));
                            }
                        }
                )
                // 乱序 延迟
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2L))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));

        dataStream.print("in");
        // 测输出流标签 {} 防止泛型查出 内部列
        OutputTag<Event> late = new OutputTag<Event>("late"){};

        SingleOutputStreamOperator<UrlCntBO> res = dataStream.keyBy(d -> d.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // 1分钟延迟 水位线查60s 内 仍然会计算
                .allowedLateness(Time.minutes(1L))
                // 没来的数据放到
                .sideOutputLateData(late)
                .aggregate(new UrlCntCase.UrlCntAgg(), new UrlCntCase.UrlCntResult());

        res.print("res");
        DataStream<Event> sideOutput = res.getSideOutput(late);
        sideOutput.print("late");
        env.execute();
    }


}
